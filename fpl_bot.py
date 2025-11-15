# fpl_bot.py — ENTERPRISE SINGLE-FILE VERSION (with /price_on)
# All improvements: async atomic I/O, canonical HMAC, state locks,
# offloaded parsing, improved retry, circuit breaker, structured logging.
# Added: /price_on YYYY-MM-DD command and command descriptions via set_my_commands.

import os
import sys
import json
import hmac
import hashlib
import asyncio
import logging
import uuid
import random
from datetime import datetime, timedelta, timezone, time
from typing import Dict, Any, Optional, List, Tuple, Set
from collections import defaultdict

import aiofiles
import httpx
from bs4 import BeautifulSoup
import fastjsonschema
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    retry_if_exception_type,
)

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------------------- SETTINGS --------------------
class Settings:
    def __init__(self):
        self.BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
        self.OWNER_ID = int(os.getenv("OWNER_ID", "0"))
        self.ALLOWED_GROUP_ID = int(os.getenv("ALLOWED_GROUP_ID", "0"))
        self.LEAGUE_ID = int(os.getenv("LEAGUE_ID", "980121"))

        self.DATA_DIR = os.getenv("DATA_DIR", "/mnt/data")
        os.makedirs(self.DATA_DIR, exist_ok=True)

        self.WHITELIST_HMAC_KEY = os.getenv("WHITELIST_HMAC_KEY", "")
        self.BOOTSTRAP_TTL = int(os.getenv("BOOTSTRAP_TTL_SECS", "30"))
        self.HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
        self.PRICE_POLL = int(os.getenv("PRICE_CHANGE_POLL_SECONDS", "300"))
        self.PRICE_UTC_PLUS = int(os.getenv("PRICE_CHANGE_UTC_PLUS", "5"))
        self.GW_DISABLE_TARGET = int(os.getenv("GW_DISABLE_TARGET", "38"))

        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

        # Files
        self.FILE_USER_TZ = f"{self.DATA_DIR}/user_timezones.json"
        self.FILE_ALLOWED = f"{self.DATA_DIR}/allowed_users.json"
        self.FILE_BASELINE = f"{self.DATA_DIR}/prices_baseline.json"
        self.FILE_LEAGUE = f"{self.DATA_DIR}/league_cache.json"
        self.FILE_NOTIF = f"{self.DATA_DIR}/notifications.json"
        self.FILE_PRICE_STATE = f"{self.DATA_DIR}/price_state.json"

        # URLs
        self.URL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
        self.URL_LIVEFPL = "https://www.livefpl.net/prices"

        if not self.BOT_TOKEN:
            print("BOT_TOKEN required", file=sys.stderr)
            sys.exit(1)
        if self.OWNER_ID <= 0:
            print("OWNER_ID required", file=sys.stderr)
            sys.exit(1)
        if self.ALLOWED_GROUP_ID == 0:
            print("ALLOWED_GROUP_ID required", file=sys.stderr)
            sys.exit(1)


settings = Settings()

# -------------------- LOGGER --------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        sk = {
            "ts": datetime.utcnow().isoformat(),
            "lvl": record.levelname,
            "msg": record.getMessage(),
            "name": record.name,
        }
        if hasattr(record, "cid"):
            sk["cid"] = record.cid
        return json.dumps(sk, ensure_ascii=False)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("fpl_bot")
logger.setLevel(settings.LOG_LEVEL)
logger.addHandler(handler)

# -------------------- UTILS --------------------

def correlation_id() -> str:
    return uuid.uuid4().hex[:12]


def canonical_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()

# -------------------- ASYNC JSON STORE --------------------
class AsyncJsonStore:
    def __init__(self, path: str):
        self.path = path
        self.lock = asyncio.Lock()

    def load_sync(self, default):
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            logger.exception("Load failed: %s", self.path)
        return default

    async def save_atomic(self, data: Any):
        async with self.lock:
            tmp = self.path + ".tmp"
            try:
                async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(data, ensure_ascii=False, indent=2))
                await asyncio.to_thread(os.replace, tmp, self.path)
            except Exception:
                logger.exception("Failed atomic save: %s", self.path)
                try:
                    if os.path.exists(tmp):
                        await asyncio.to_thread(os.remove, tmp)
                except Exception:
                    pass


store_user_tz = AsyncJsonStore(settings.FILE_USER_TZ)
store_allowed = AsyncJsonStore(settings.FILE_ALLOWED)
store_baseline = AsyncJsonStore(settings.FILE_BASELINE)
store_league = AsyncJsonStore(settings.FILE_LEAGUE)
store_notif = AsyncJsonStore(settings.FILE_NOTIF)
store_state = AsyncJsonStore(settings.FILE_PRICE_STATE)

# initial sync loads
user_timezones: Dict[int, str] = store_user_tz.load_sync({})
allowed_users: Set[int] = set()
baseline = store_baseline.load_sync({})
league_cache = store_league.load_sync({})
notif_flag = store_notif.load_sync({"enabled": True})
price_state = store_state.load_sync({})

# -------------------- WHITELIST HMAC --------------------

def whitelist_pack(users: List[int]) -> dict:
    users_sorted = sorted(int(u) for u in users)
    payload = {"users": users_sorted}
    if not settings.WHITELIST_HMAC_KEY:
        return {"payload": payload, "sig": "", "schema": 1}

    sig = hmac.new(
        settings.WHITELIST_HMAC_KEY.encode(), canonical_bytes(users_sorted), hashlib.sha256
    ).hexdigest()
    return {"payload": payload, "sig": sig, "schema": 1}


def whitelist_unpack(blob: dict) -> List[int]:
    raw = blob.get("payload", {}).get("users", [])
    sig = blob.get("sig", "")
    if not settings.WHITELIST_HMAC_KEY:
        return raw
    expected = hmac.new(
        settings.WHITELIST_HMAC_KEY.encode(), canonical_bytes(raw), hashlib.sha256
    ).hexdigest()
    if not sig or not hmac.compare_digest(sig, expected):
        logger.warning("Whitelist signature mismatch")
        return []
    return raw


if os.path.exists(settings.FILE_ALLOWED):
    blob = store_allowed.load_sync({})
    allowed_users = set(whitelist_unpack(blob))

# -------------------- STATE LOCKS --------------------
lock_allowed = asyncio.Lock()
lock_tz = asyncio.Lock()
lock_baseline = asyncio.Lock()
lock_league = asyncio.Lock()
lock_notif = asyncio.Lock()
lock_state = asyncio.Lock()

# -------------------- HTTP CLIENT --------------------
class HttpClient:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=settings.HTTP_TIMEOUT, headers={"User-Agent": "FPLBot/Enterprise"})

    @retry(
        retry=(
            retry_if_exception_type(httpx.TransportError)
            | retry_if_exception(lambda e: isinstance(e, httpx.HTTPStatusError) and e.response.status_code in (429, 502, 503, 504))
        ),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(4),
    )
    async def get(self, url: str) -> httpx.Response:
        resp = await self.client.get(url)
        if resp.status_code in (429, 502, 503, 504):
            raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
        return resp

    async def get_json(self, url: str) -> Optional[dict]:
        try:
            resp = await self.get(url)
            return resp.json()
        except Exception:
            logger.exception("get_json fail %s", url)
            return None

    async def get_text(self, url: str) -> Optional[str]:
        try:
            resp = await self.get(url)
            return resp.text
        except Exception:
            logger.exception("get_text fail %s", url)
            return None

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass


http = HttpClient()

# -------------------- CIRCUIT BREAKER --------------------
class CircuitBreaker:
    def __init__(self, threshold=3, cooldown=600):
        self.threshold = threshold
        self.cooldown = cooldown
        self.failures = 0
        self.open_until = 0

    def is_open(self):
        now = datetime.now().timestamp()
        if self.open_until == 0:
            return False
        if now >= self.open_until:
            self.failures = 0
            self.open_until = 0
            return False
        return True

    def record_success(self):
        self.failures = 0
        self.open_until = 0

    def record_failure(self):
        self.failures += 1
        if self.failures >= self.threshold:
            jitter = random.randint(0, 30)
            self.open_until = datetime.now().timestamp() + self.cooldown + jitter
            logger.warning("Circuit breaker opened")

# -------------------- BOOTSTRAP SERVICE --------------------
class BootstrapService:
    def __init__(self):
        self.cache = {"ts": 0.0, "data": None, "elements": [], "el_map": {}, "name_index": {}}

    async def get(self, force=False):
        now = asyncio.get_event_loop().time()
        if not force and self.cache["data"] and now - self.cache["ts"] < settings.BOOTSTRAP_TTL:
            return self.cache["data"]
        data = await http.get_json(settings.URL_BOOTSTRAP)
        if not data:
            return None
        elements = data.get("elements", [])
        name_index = defaultdict(list)
        el_map = {}
        for el in elements:
            try:
                eid = int(el["id"])
                el_map[eid] = el
                key = el.get("web_name", "").lower()
                name_index[key].append(el)
            except Exception:
                pass
        self.cache.update({"ts": now, "data": data, "elements": elements, "el_map": el_map, "name_index": name_index})
        return data


bootstrap_service = BootstrapService()

# -------------------- LIVEFPL SERVICE --------------------
LIVEFPL_SCHEMA = {
    "type": "object",
    "properties": {"Name": {"type": "string"}, "Price": {"type": "string"}, "Target": {"type": "string"}},
    "required": ["Name", "Price", "Target"],
}
validate_livefpl = fastjsonschema.compile(LIVEFPL_SCHEMA)

class LiveFPLService:
    def __init__(self):
        self.cb = CircuitBreaker()

    async def fetch_sections(self):
        if self.cb.is_open():
            return None
        html = await http.get_text(settings.URL_LIVEFPL)
        if not html:
            self.cb.record_failure()
            return None
        self.cb.record_success()
        sections = await asyncio.to_thread(self._parse_html_sync, html)
        return sections

    @staticmethod
    def _parse_html_sync(html: str):
        soup = BeautifulSoup(html, "lxml")
        def parse_table(key: str):
            out = []
            hdr = soup.find(lambda t: t.name in ("h3", "h2", "strong") and key.lower() in t.get_text(" ", strip=True).lower())
            if not hdr:
                return out
            table = hdr.find_next("table")
            if not table:
                return out
            for tr in table.find_all("tr"):
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(tds) >= 3:
                    row = {"Name": tds[0], "Price": tds[-2], "Target": tds[-1]}
                    try:
                        validate_livefpl(row)
                        out.append(row)
                    except Exception:
                        pass
            return out
        return {"Already reached target": parse_table("Already reached"), "Projected to reach target": parse_table("Projected"), "Others who will be close": parse_table("Others")}


livefpl_service = LiveFPLService()

# -------------------- LEAGUE SERVICE --------------------
class LeagueService:
    async def get_current_gw(self):
        data = await bootstrap_service.get()
        if not data:
            return 0
        for ev in data.get("events", []):
            if ev.get("is_current"):
                return int(ev["id"]) or 0
        return 0

    async def get_league_players(self):
        async with lock_league:
            now_gw = await self.get_current_gw()
            cached = league_cache.get("gw")
            if cached == now_gw and league_cache.get("players"):
                return set(league_cache["players"])
            players = await self._fetch_league(settings.LEAGUE_ID)
            league_cache["gw"] = now_gw
            league_cache["players"] = list(players)
            await store_league.save_atomic(league_cache)
            return players

    async def _fetch_league(self, league_id: int):
        url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
        data = await http.get_json(url)
        result = set()
        if not data:
            return result
        entries = [r.get("entry") for r in (data.get("standings", {}).get("results", []) or []) if r.get("entry")]
        sem = asyncio.Semaphore(8)
        async def fetch_entry(eid: int):
            async with sem:
                j = await http.get_json(f"https://fantasy.premierleague.com/api/entry/{eid}/event/0/picks/")
                if not j:
                    return
                for p in j.get("picks", []):
                    el = p.get("element")
                    if el:
                        result.add(f"id:{el}")
        tasks = [asyncio.create_task(fetch_entry(e)) for e in entries[:80]]
        await asyncio.gather(*tasks, return_exceptions=True)
        return result


league_service = LeagueService()

# -------------------- PRICE DETECTOR SERVICE --------------------
class PriceDetectorService:
    async def build_prices_msg(self):
        sections = await livefpl_service.fetch_sections()
        if not sections:
            return "<code>LiveFPL unavailable</code>"
        text = []
        for title, rows in sections.items():
            text.append(f"<b>{title}</b>")
            if not rows:
                text.append("<code>(none)</code>")
            else:
                for r in rows:
                    text.append(f"<code>{r['Name'].ljust(15)} £{r['Price']} ({r['Target']})</code>")
        return "\n".join(text)

    async def detect_changes(self):
        data = await bootstrap_service.get()
        if not data:
            return None
        elements = data.get("elements", [])
        new_map = {str(el["id"]): el["now_cost"] for el in elements}
        async with lock_baseline:
            changes = []
            for eid, new_cost in new_map.items():
                old = baseline.get(eid)
                if old is not None and old != new_cost:
                    changes.append((eid, old, new_cost))
            if changes:
                # save baseline and also daily snapshot
                await store_baseline.save_atomic(new_map)
                baseline.clear()
                baseline.update(new_map)
                # save daily snapshot
                today_str = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=settings.PRICE_UTC_PLUS))).date().isoformat()
                daily_path = os.path.join(settings.DATA_DIR, f"prices_baseline_{today_str}.json")
                try:
                    async with aiofiles.open(daily_path, "w", encoding="utf-8") as f:
                        await f.write(json.dumps(new_map, ensure_ascii=False, indent=2))
                except Exception:
                    logger.exception("Failed saving daily baseline %s", daily_path)
                return self._format_changes(changes, bootstrap_service.cache.get("el_map", {}))
            if not baseline:
                baseline.update(new_map)
                await store_baseline.save_atomic(baseline)
        return None

    def _format_changes(self, changes, el_map):
        lines = ["<b>Price changes:</b>"]
        for eid, o, n in changes:
            el = el_map.get(int(eid), {})
            nm = el.get("web_name", f"id:{eid}")
            lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} → {n/10:.1f}</code>")
        return "\n".join(lines)

    # helper for historical compare
    def detect_between_maps(self, m1: Dict[str, int], m2: Dict[str, int]) -> List[Tuple[str, int, int]]:
        changes = []
        for eid, new_cost in m2.items():
            old = m1.get(eid)
            if old is not None and old != new_cost:
                changes.append((eid, old, new_cost))
        return changes


price_service = PriceDetectorService()

# -------------------- AUTH --------------------
class AuthService:
    async def is_authorized(self, update: Update) -> bool:
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user:
            return False
        uid = user.id
        if uid == settings.OWNER_ID and chat.type == "private":
            return True
        if chat.id == settings.ALLOWED_GROUP_ID:
            async with lock_allowed:
                if uid not in allowed_users:
                    allowed_users.add(uid)
                    blob = whitelist_pack(list(allowed_users))
                    await store_allowed.save_atomic(blob)
            return True
        if chat.type == "private":
            async with lock_allowed:
                return uid == settings.OWNER_ID or uid in allowed_users
        return False


auth = AuthService()

# -------------------- HELPERS: daily baseline load --------------------
def load_daily_baseline(date_str: str) -> Optional[Dict[str, int]]:
    path = os.path.join(settings.DATA_DIR, f"prices_baseline_{date_str}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
        # ensure values ints
        return {str(k): int(v) for k, v in j.items()}
    except Exception:
        logger.exception("Failed to load daily baseline %s", path)
        return None

# -------------------- TELEGRAM HANDLERS --------------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    msg = (
        "FPL Enterprise Bot\n"
        "/prices — show today\'s LiveFPL prices\n"
        "/price_on YYYY-MM-DD — show price changes between that date and next day\n"
        "/settz <zone> — set timezone (IANA or +N)\n"
        "/mytz — view timezone\n"
        "/notify_status — notification state\n"
    )
    await update.message.reply_text(msg)


async def cmd_prices(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    msg = await price_service.build_prices_msg()
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_price_on(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /price_on YYYY-MM-DD")
        return
    date_str = args[0].strip()
    try:
        dt = datetime.fromisoformat(date_str)
    except Exception:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
        return
    day1 = load_daily_baseline(date_str)
    if not day1:
        await update.message.reply_text(f"No baseline file for {date_str}")
        return
    next_day = (dt.date() + timedelta(days=1)).isoformat()
    day2 = load_daily_baseline(next_day)
    if not day2:
        await update.message.reply_text(f"No baseline for next day ({next_day}). Need both days to compare.")
        return
    changes = price_service.detect_between_maps(day1, day2)
    el_map = bootstrap_service.cache.get("el_map", {})
    if not changes:
        await update.message.reply_text(f"<code>No changes between {date_str} and {next_day}</code>", parse_mode="HTML")
        return
    # format
    lines = [f"<b>Price changes {date_str} → {next_day}</b>"]
    for eid, o, n in changes:
        el = el_map.get(int(eid), {})
        nm = el.get("web_name", f"id:{eid}")
        lines.append(f"<code>{nm.ljust(15)} {o/10:.1f} → {n/10:.1f}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_settz(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    args = ctx.args or []
    if not args:
        async with lock_tz:
            tz = user_timezones.get(update.effective_user.id, f"UTC+{settings.PRICE_UTC_PLUS}")
        await update.message.reply_text(f"Usage: /settz <zone>\nCurrent={tz}")
        return
    raw = args[0]
    parsed = None
    if (raw.startswith("+") or raw.startswith("-")) and raw[1:].isdigit():
        parsed = f"UTC{raw}"
    else:
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(raw)
            parsed = raw
        except Exception:
            parsed = None
    if not parsed:
        await update.message.reply_text("Invalid timezone")
        return
    async with lock_tz:
        user_timezones[update.effective_user.id] = parsed
        await store_user_tz.save_atomic({str(k): v for k, v in user_timezones.items()})
    await update.message.reply_text(f"Timezone set to {parsed}")


async def cmd_mytz(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    async with lock_tz:
        tz = user_timezones.get(update.effective_user.id, f"UTC+{settings.PRICE_UTC_PLUS}")
    await update.message.reply_text(f"Timezone: {tz}")


async def cmd_notify_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await auth.is_authorized(update):
        return
    async with lock_notif:
        enabled = notif_flag.get("enabled", True)
    await update.message.reply_text(f"Notifications: {enabled}")


async def _noop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.id == settings.ALLOWED_GROUP_ID:
        await auth.is_authorized(update)

# -------------------- SCHEDULER --------------------
class Scheduler:
    async def run_daily(self, app: Application):
        while True:
            dt = self._next_local_utc(settings.PRICE_UTC_PLUS, 23, 0)
            await self._sleep_until(dt)
            msg = await price_service.build_prices_msg()
            await app.bot.send_message(settings.ALLOWED_GROUP_ID, msg, parse_mode="HTML")
            await asyncio.sleep(1)

    async def run_price_detector(self, app: Application):
        while True:
            start, end = self._price_window()
            now = datetime.now(timezone.utc)
            if now < start:
                await self._sleep_until(start)
            async with lock_state:
                done_today = price_state.get("last_checked_date") == now.date().isoformat()
            if done_today:
                next_day = start + timedelta(days=1)
                await self._sleep_until(next_day)
                continue
            found = False
            while datetime.now(timezone.utc) <= end:
                msg = await price_service.detect_changes()
                if msg:
                    await app.bot.send_message(settings.ALLOWED_GROUP_ID, msg, parse_mode="HTML")
                    found = True
                    break
                await asyncio.sleep(settings.PRICE_POLL)
            if not found:
                await app.bot.send_message(settings.ALLOWED_GROUP_ID, "<code>No price changes</code>", parse_mode="HTML")
            async with lock_state:
                price_state["last_checked_date"] = now.date().isoformat()
                await store_state.save_atomic(price_state)
            next_day = start + timedelta(days=1)
            await self._sleep_until(next_day)

    async def _sleep_until(self, dt: datetime):
        while True:
            now = datetime.now(timezone.utc)
            left = (dt - now).total_seconds()
            if left <= 0:
                return
            await asyncio.sleep(min(left, 60))

    def _next_local_utc(self, offset: int, hour: int, minute: int):
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(timezone.utc).astimezone(tz)
        base = datetime.combine(now.date(), time(hour, minute), tzinfo=tz)
        if now >= base:
            base += timedelta(days=1)
        return base.astimezone(timezone.utc)

    def _price_window(self):
        tz = timezone(timedelta(hours=settings.PRICE_UTC_PLUS))
        now = datetime.now(timezone.utc).astimezone(tz)
        start = datetime.combine(now.date(), time(5, 45), tzinfo=tz)
        end = datetime.combine(now.date(), time(8, 0), tzinfo=tz)
        return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


scheduler = Scheduler()

# -------------------- BOT APP --------------------
class BotApp:
    def __init__(self):
        self.app = Application.builder().token(settings.BOT_TOKEN).build()
        self.app.add_handler(CommandHandler("start", cmd_start))
        self.app.add_handler(CommandHandler("prices", cmd_prices))
        self.app.add_handler(CommandHandler("price_on", cmd_price_on))
        self.app.add_handler(CommandHandler("settz", cmd_settz))
        self.app.add_handler(CommandHandler("mytz", cmd_mytz))
        self.app.add_handler(CommandHandler("notify_status", cmd_notify_status))
        self.app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), _noop))

        async def on_start(app):
            # register command descriptions (visible in Telegram UI)
            try:
                await app.bot.set_my_commands([
                    ("start", "Show help and available commands"),
                    ("prices", "Show today's LiveFPL price projections"),
                    ("price_on", "Show price changes for a given date (YYYY-MM-DD)"),
                    ("settz", "Set your timezone (IANA or +N)"),
                    ("mytz", "Show your timezone"),
                    ("notify_status", "Show notification enabled state"),
                ])
            except Exception:
                logger.exception("Failed to set bot commands")
            asyncio.create_task(scheduler.run_daily(app))
            asyncio.create_task(scheduler.run_price_detector(app))

        async def on_stop(app):
            await http.close()

        self.app.post_init = on_start
        self.app.post_shutdown = on_stop

    def run(self):
        self.app.run_polling()


# -------------------- ENTRYPOINT --------------------

def main():
    BotApp().run()


if __name__ == "__main__":
    main()
