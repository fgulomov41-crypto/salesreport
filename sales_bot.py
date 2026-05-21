#!/usr/bin/env python3
"""
Bitrix24 Sales Report Telegram Bot
Ежедневные и недельные отчёты по активности сейлзов
Команды: /daily, /weekly, /debug, /chatid
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── КОНФИГУРАЦИЯ ───────────────────────────────────────────────────────────
# Значения берутся из переменных окружения (Railway) или из defaults ниже

BITRIX_WEBHOOK   = os.getenv("BITRIX_WEBHOOK",   "https://bitrix.uzum.com/rest/297435/2jygolso75y7ozjb/")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "8319953238:AAHsGW0vZYYAgQpPdk3DsE6b_Rerzg8Z-UI")
_chat_ids_env    = os.getenv("ALLOWED_CHAT_IDS",  "-1002830183416")
ALLOWED_CHAT_IDS : List[int] = [int(x.strip()) for x in _chat_ids_env.split(",") if x.strip()]

EMPLOYEES: Dict[int, str] = {
    389012: "Akmaljon Xolmatov",
    399780: "Maruf Saburov",
    398172: "Suxrob Abduraxmonov",
}

PIPELINE_SALES_ID = 64
PIPELINE_DEV_ID   = 58

# ─── ID ЭТАПОВ ───────────────────────────────────────────────────────────────
# Воронка Продажи (cat 64)
STAGE_SALES_DELETION = "C64:UC_PIVQTV"   # На удаление

# Воронка Развитие (cat 58)
STAGE_DEV_NACHALO    = "C58:NEW"
STAGE_DEV_SBOR       = "C58:PREPARATION"
STAGE_DEV_TEH_VAL    = "C58:PREPAYMENT_INVOIC"
STAGE_DEV_DORABOTKI  = "C58:UC_LGK1JM"
STAGE_DEV_FINAL      = "C58:FINAL_INVOICE"
STAGE_DEV_TEST_ORDER = "C58:UC_PRSTUZ"

# Причины удаления (ищутся в комментариях)
DELETION_REASONS: Dict[str, List[str]] = {
    "НДЗ":        ["ндз", "не дозвон", "недозвон"],
    "Спам":       ["спам", "spam"],
    "Дубль":      ["дубль", "дубликат"],
    "Не целевой": ["не целев", "нецелев"],
    "Отказ":      ["отказ"],
}

# "Договорились" — ищем эти слова в комментариях к сделке
AGREED_KEYWORDS = ["договорились", "жду реквизит", "реквизиты", "ждем реквизит"]

# ─── ЛОГИРОВАНИЕ ─────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── BITRIX24 API ─────────────────────────────────────────────────────────────

async def bx_post(session: aiohttp.ClientSession, method: str, params: dict) -> dict:
    url = BITRIX_WEBHOOK + method
    try:
        async with session.post(url, json=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json(content_type=None)
    except Exception as e:
        logger.error(f"HTTP error [{method}]: {e}")
        return {}


async def bx_call(session: aiohttp.ClientSession, method: str, params: dict = None) -> any:
    data = await bx_post(session, method, params or {})
    if "error" in data:
        logger.warning(f"BX [{method}] error: {data.get('error')} — {data.get('error_description','')}")
    return data.get("result", [])


async def bx_list_all(session: aiohttp.ClientSession, method: str, params: dict = None) -> List[dict]:
    """Все записи с пагинацией"""
    results, start, params = [], 0, (params or {})
    while True:
        data = await bx_post(session, method, {**params, "start": start})
        batch = data.get("result", [])
        if not isinstance(batch, list) or not batch:
            break
        results.extend(batch)
        total = int(data.get("total", 0))
        start += 50
        if start >= total or len(batch) < 50:
            break
    return results


async def bx_batch(session: aiohttp.ClientSession, commands: Dict[str, str]) -> Dict[str, any]:
    data  = await bx_post(session, "batch", {"halt": 0, "cmd": commands})
    outer = data.get("result", {})
    if not isinstance(outer, dict):
        return {}
    inner = outer.get("result", {})
    if not isinstance(inner, dict):
        return {}
    return inner


# ─── ПОЛУЧЕНИЕ СДЕЛОК ────────────────────────────────────────────────────────

async def get_modified_deals(
    session:     aiohttp.ClientSession,
    date_from:   datetime,
    date_to:     datetime,
    category_id: int,
    extra_filter: dict = None,
) -> List[dict]:
    """Сделки категории, изменённые за период"""
    f = {
        "CATEGORY_ID": str(category_id),
        ">=DATE_MODIFY": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
        "<=DATE_MODIFY": date_to.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if extra_filter:
        f.update(extra_filter)
    return await bx_list_all(session, "crm.deal.list", {
        "filter": f,
        "select": ["ID", "TITLE", "ASSIGNED_BY_ID", "STAGE_ID", "DATE_MODIFY", "DATE_CREATE"],
    })


async def get_created_deals(
    session:     aiohttp.ClientSession,
    date_from:   datetime,
    date_to:     datetime,
    category_id: int,
) -> List[dict]:
    """Сделки категории, СОЗДАННЫЕ за период (переводы из другой воронки)"""
    return await bx_list_all(session, "crm.deal.list", {
        "filter": {
            "CATEGORY_ID": str(category_id),
            ">=DATE_CREATE": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
            "<=DATE_CREATE": date_to.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "select": ["ID", "TITLE", "ASSIGNED_BY_ID", "STAGE_ID", "DATE_CREATE"],
    })


# ─── КОММЕНТАРИИ К СДЕЛКАМ ───────────────────────────────────────────────────

async def fetch_comments_for_deals(
    session:   aiohttp.ClientSession,
    deal_ids:  List[str],
    date_from: datetime,
    date_to:   datetime,
) -> Dict[str, List[str]]:
    """
    Получить все комментарии к сделкам за период.
    Возвращает {deal_id: [текст комментария, ...]}
    """
    if not deal_ids:
        return {}

    result_map: Dict[str, List[str]] = {}
    batch_size = 49

    for i in range(0, len(deal_ids), batch_size):
        chunk = deal_ids[i : i + batch_size]
        commands = {
            f"c{j}": (
                f"crm.timeline.comment.list"
                f"?filter[ENTITY_TYPE]=deal"
                f"&filter[ENTITY_ID]={did}"
                f"&order[ID]=DESC"
            )
            for j, did in enumerate(chunk)
        }
        batch_result = await bx_batch(session, commands)

        for key, items in batch_result.items():
            idx     = int(key[1:])
            deal_id = chunk[idx]
            if not isinstance(items, list):
                continue
            texts = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                created_raw = str(item.get("CREATED", "") or "")
                # Фильтруем по дате
                try:
                    dt = datetime.fromisoformat(created_raw[:19])
                    if dt < date_from or dt > date_to:
                        continue
                except (ValueError, TypeError):
                    pass  # если дата не парсится — включаем всё
                text = str(item.get("COMMENT", "") or "")
                if text:
                    texts.append(text.lower())
            if texts:
                result_map[deal_id] = texts

    return result_map


def get_deletion_reason(texts: List[str]) -> str:
    combined = " ".join(texts)
    for reason, keywords in DELETION_REASONS.items():
        for kw in keywords:
            if kw in combined:
                return reason
    return "Другое"


def has_agreed(texts: List[str]) -> bool:
    combined = " ".join(texts)
    return any(kw in combined for kw in AGREED_KEYWORDS)


# ─── ФОРМИРОВАНИЕ ОТЧЁТА ─────────────────────────────────────────────────────

async def generate_report(date_from: datetime, date_to: datetime, period_label: str) -> str:
    async with aiohttp.ClientSession() as session:

        # ── 1. Сделки Продажи, изменённые за период ─────────────────────────
        sales_deals = await get_modified_deals(session, date_from, date_to, PIPELINE_SALES_ID)
        logger.info(f"Sales deals: {len(sales_deals)}")

        # ── 2. Сделки Развитие, изменённые за период ────────────────────────
        dev_deals = await get_modified_deals(session, date_from, date_to, PIPELINE_DEV_ID)
        logger.info(f"Dev deals: {len(dev_deals)}")

        # ── 3. Сделки Развитие, СОЗДАННЫЕ за период (переведены из Продажи) ─
        dev_created = await get_created_deals(session, date_from, date_to, PIPELINE_DEV_ID)
        logger.info(f"Dev deals created today: {len(dev_created)}")

        # ── 4. Инициализация счётчиков ────────────────────────────────────────
        stats: Dict[int, dict] = {
            eid: {
                "total_sales":    0,
                "deletion":       {},    # reason → count
                "deletion_total": 0,
                "agreed":         0,
                "moved_to_dev":   0,
                "начало":         0,
                "сбор_инфо":      0,
                "тех_валидация":  0,
                "доработки":      0,     # сейчас в доработках
                "финал":          0,
                "тест_заказ":     0,
            }
            for eid in EMPLOYEES
        }

        # ── 5. Разбор Продажи по текущему этапу ─────────────────────────────
        sales_del_ids: List[str]      = []
        sales_all_ids: List[str]      = []
        sales_deal_emp: Dict[str,int] = {}

        for d in sales_deals:
            emp_id = int(d.get("ASSIGNED_BY_ID", 0))
            if emp_id not in EMPLOYEES:
                continue
            did   = d["ID"]
            stage = d.get("STAGE_ID", "")
            sales_deal_emp[did] = emp_id
            sales_all_ids.append(did)
            stats[emp_id]["total_sales"] += 1
            if stage == STAGE_SALES_DELETION:
                sales_del_ids.append(did)
                stats[emp_id]["deletion_total"] += 1

        # ── 6. Комментарии к удалённым сделкам (причины) ─────────────────────
        if sales_del_ids:
            del_comments = await fetch_comments_for_deals(session, sales_del_ids, date_from, date_to)
            for did in sales_del_ids:
                emp_id = sales_deal_emp.get(did)
                if emp_id not in EMPLOYEES:
                    continue
                texts  = del_comments.get(did, [])
                reason = get_deletion_reason(texts)
                rc     = stats[emp_id]["deletion"]
                rc[reason] = rc.get(reason, 0) + 1

        # ── 7. "Договорились" — ищем по комментариям Продажи ─────────────────
        if sales_all_ids:
            all_comments = await fetch_comments_for_deals(session, sales_all_ids, date_from, date_to)
            for did, texts in all_comments.items():
                emp_id = sales_deal_emp.get(did)
                if emp_id not in EMPLOYEES:
                    continue
                if has_agreed(texts):
                    stats[emp_id]["agreed"] += 1

        # ── 8. Переводы из Продажи → Развитие ────────────────────────────────
        for d in dev_created:
            emp_id = int(d.get("ASSIGNED_BY_ID", 0))
            if emp_id in EMPLOYEES:
                stats[emp_id]["moved_to_dev"] += 1

        # ── 9. Разбор Развитие по текущему этапу ─────────────────────────────
        for d in dev_deals:
            emp_id = int(d.get("ASSIGNED_BY_ID", 0))
            if emp_id not in EMPLOYEES:
                continue
            stage = d.get("STAGE_ID", "")
            if   stage == STAGE_DEV_NACHALO:    stats[emp_id]["начало"]        += 1
            elif stage == STAGE_DEV_SBOR:       stats[emp_id]["сбор_инфо"]     += 1
            elif stage == STAGE_DEV_TEH_VAL:    stats[emp_id]["тех_валидация"] += 1
            elif stage == STAGE_DEV_DORABOTKI:  stats[emp_id]["доработки"]     += 1
            elif stage == STAGE_DEV_FINAL:      stats[emp_id]["финал"]         += 1
            elif stage == STAGE_DEV_TEST_ORDER: stats[emp_id]["тест_заказ"]    += 1

    # ── ФОРМАТИРОВАНИЕ ────────────────────────────────────────────────────────
    def e(t: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            t = t.replace(ch, f"\\{ch}")
        return t

    lines = [
        f"📊 *Отчёт — {e(period_label)}*",
        f"🗓 {e(date_from.strftime('%d.%m.%Y'))} — {e(date_to.strftime('%d.%m.%Y'))}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for eid, name in EMPLOYEES.items():
        s = stats[eid]
        del_detail = ""
        if s["deletion"]:
            parts = ", ".join(f"{e(r)}: {c}" for r, c in s["deletion"].items())
            del_detail = f" \\({parts}\\)"

        lines.append(f"\n👤 *{e(name)}*")
        lines.append(f"  📋 Активных сделок \\(Продажи\\): *{s['total_sales']}*")
        lines.append(f"  🗑 На удаление: *{s['deletion_total']}*{del_detail}")
        lines.append(f"  🤝 Договорились / жду реквизиты: *{s['agreed']}*")
        lines.append(f"  🔄 Переведено в Тезкор Развитие: *{s['moved_to_dev']}*")
        lines.append(f"  ─────────────────────────────")
        lines.append(f"  🟢 Начало: *{s['начало']}*")
        lines.append(f"  📝 Сбор информации: *{s['сбор_инфо']}*")
        lines.append(f"  🔬 Тех\\. валидация: *{s['тех_валидация']}*")
        lines.append(f"  🔧 В доработках: *{s['доработки']}*")
        lines.append(f"  ✅ Финальная проверка: *{s['финал']}*")
        lines.append(f"  🧪 Тестовый заказ: *{s['тест_заказ']}*")

    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("_Сформировано автоматически_")
    return "\n".join(lines)


# ─── TELEGRAM HANDLERS ───────────────────────────────────────────────────────

def is_allowed(update: Update) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True
    return update.effective_chat.id in ALLOWED_CHAT_IDS


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "👋 Привет! Бот отчётности Tezkor Sales.\n\n"
        "/daily — отчёт за сегодня\n"
        "/weekly — отчёт за текущую неделю (пн–сегодня)\n"
        "/debug — диагностика\n"
        "/chatid — ID этого чата"
    )


async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(
        f"Chat ID: `{chat.id}`\nТип: {chat.type}\nНазвание: {chat.title or chat.first_name or '—'}",
        parse_mode="Markdown",
    )


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text("⏳ Формирую дневной отчёт…")
    now       = datetime.now()
    date_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        report = await generate_report(date_from, now, f"сегодня {now.strftime('%d.%m.%Y')}")
        await update.message.reply_text(report, parse_mode="MarkdownV2")
    except Exception as e:
        logger.exception("Error in /daily")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text("⏳ Формирую недельный отчёт…")
    now    = datetime.now()
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    label  = f"неделя {monday.strftime('%d.%m')}–{now.strftime('%d.%m.%Y')}"
    try:
        report = await generate_report(monday, now, label)
        await update.message.reply_text(report, parse_mode="MarkdownV2")
    except Exception as e:
        logger.exception("Error in /weekly")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика"""
    if not is_allowed(update):
        return
    await update.message.reply_text("🔍 Диагностика…")
    now       = datetime.now()
    date_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
    lines     = [f"🕐 {now.strftime('%Y-%m-%d %H:%M:%S')}\n"]

    try:
        async with aiohttp.ClientSession() as session:

            sales_deals = await get_modified_deals(session, date_from, now, PIPELINE_SALES_ID)
            lines.append(f"📦 Продажи изменено сегодня: {len(sales_deals)}")
            by_stage: Dict[str,int] = {}
            by_emp:   Dict[str,int] = {}
            for d in sales_deals:
                s = d.get("STAGE_ID","?")
                e = d.get("ASSIGNED_BY_ID","?")
                by_stage[s] = by_stage.get(s, 0) + 1
                by_emp[str(e)] = by_emp.get(str(e), 0) + 1
            for s,c in sorted(by_stage.items(), key=lambda x: -x[1])[:5]:
                lines.append(f"  {s}: {c}")
            lines.append(f"  По сотрудникам: {by_emp}")

            dev_deals = await get_modified_deals(session, date_from, now, PIPELINE_DEV_ID)
            lines.append(f"\n📦 Развитие изменено сегодня: {len(dev_deals)}")
            by_stage2: Dict[str,int] = {}
            for d in dev_deals:
                s = d.get("STAGE_ID","?")
                by_stage2[s] = by_stage2.get(s, 0) + 1
            for s,c in sorted(by_stage2.items(), key=lambda x: -x[1])[:6]:
                lines.append(f"  {s}: {c}")

            dev_created = await get_created_deals(session, date_from, now, PIPELINE_DEV_ID)
            lines.append(f"\n🆕 Развитие создано сегодня: {len(dev_created)}")

            # Проверяем комментарии на одной сделке
            if sales_deals:
                test_id = sales_deals[0]["ID"]
                comments = await fetch_comments_for_deals(session, [test_id], date_from, now)
                lines.append(f"\n💬 Комментарии сделки [{test_id}]: {len(comments.get(test_id, []))} шт")
                for t in comments.get(test_id, [])[:2]:
                    lines.append(f"  «{t[:60]}»")

    except Exception as ex:
        lines.append(f"\n❌ Ошибка: {ex}")
        logger.exception("debug error")

    await update.message.reply_text("\n".join(lines))


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    logger.info("Starting Sales Report Bot…")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("daily",  cmd_daily))
    app.add_handler(CommandHandler("weekly", cmd_weekly))
    app.add_handler(CommandHandler("debug",  cmd_debug))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
