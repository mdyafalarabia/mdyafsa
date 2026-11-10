"""FKRAH TikTok LIVE collector: relay comments, joins, gifts, and question queue."""
import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, GiftEvent, JoinEvent

CHANNEL = "fkrahchannel"
PRIMARY_URL = os.environ.get("FKRAH_RELAY_URL", "https://fkrah-live-events.mdyafalarabia.workers.dev/ingest")
FALLBACK_URL = os.environ.get("FKRAH_FALLBACK_URL", "https://fkrah-smart-majlis-demo-mdyaf.vercel.app/api/live-events")
TOKEN = os.environ.get("FKRAH_INGEST_TOKEN", "").strip()
log = logging.getLogger("fkrah.collector")
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

state = {
    "channel": CHANNEL, "status": "starting", "room_connected": False,
    "events_sent": 0, "events_failed": 0, "gifts_sent": 0, "joins_sent": 0,
    "questions_sent": 0, "comments_sent": 0, "last_attempt_at": None, "last_connect_at": None,
    "last_relay_at": None, "last_error": None, "relay_status": "not_tested",
    "last_http_status": None
}
join_seen = {}
last_join_at = 0.0

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def avatar_url(user):
    for field in ("avatar_thumb", "avatar_medium", "avatar_large"):
        obj = getattr(user, field, None)
        for url in (getattr(obj, "url_list", None) or []):
            if isinstance(url, str) and url.startswith("https://"):
                return url[:600]
    return ""

def identity(user):
    if user is None:
        return None
    username = str(getattr(user, "unique_id", "") or "").lstrip("@")[:60]
    if not username:
        return None
    return {
        "username": username,
        "nickname": str(getattr(user, "nickname", "") or username)[:70],
        "avatar": avatar_url(user)
    }

def post_sync(payload):
    if not TOKEN:
        raise RuntimeError("FKRAH_INGEST_TOKEN not configured")
    body = json.dumps({"channel": CHANNEL, "source": "tiktok", **payload}, ensure_ascii=False).encode("utf-8")
    errors = []
    for url in dict.fromkeys((PRIMARY_URL, FALLBACK_URL)):
        if not url.startswith("https://"):
            continue
        request = Request(
            url, method="POST", data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer " + TOKEN,
                "User-Agent": "FKRAH-TikTok-LIVE-Collector/2.0"
            }
        )
        try:
            with urlopen(request, timeout=13) as response:
                result = json.load(response)
                if not result.get("ok"):
                    raise RuntimeError("relay rejected event")
                return url
        except HTTPError as exc:
            state["last_http_status"] = exc.code
            errors.append("HTTP " + str(exc.code))
        except (URLError, TimeoutError, RuntimeError, OSError) as exc:
            errors.append(type(exc).__name__)
    raise RuntimeError("All relay routes failed: " + ", ".join(errors))

async def relay(payload):
    try:
        selected = await asyncio.to_thread(post_sync, payload)
        state["relay_status"] = "connected"
        state["events_sent"] += 1
        state["last_relay_at"] = utcnow()
        state["last_error"] = None
        if payload.get("type") == "gift":
            state["gifts_sent"] += 1
        if payload.get("type") == "join":
            state["joins_sent"] += 1
        if payload.get("type") == "comment":
            state["comments_sent"] += 1
            if str(payload.get("comment", "")).startswith("سؤال"):
                state["questions_sent"] += 1
        return True
    except Exception as exc:
        state["events_failed"] += 1
        state["relay_status"] = "error"
        state["last_error"] = str(exc)[:150]
        log.warning("Relay problem: %s", state["last_error"])
        return False

async def run_one():
    global last_join_at
    state["room_connected"] = False
    state["status"] = "checking_room"
    state["last_attempt_at"] = utcnow()
    client = TikTokLiveClient(unique_id="@" + CHANNEL)

    @client.on(ConnectEvent)
    async def on_connect(event):
        state["status"] = "connected"
        state["room_connected"] = True
        state["last_connect_at"] = utcnow()

    @client.on(CommentEvent)
    async def on_comment(event):
        msg = str(getattr(event, "comment", "") or "").strip()[:350]
        # Receive every real public text comment. Categorization and moderation occur downstream.
        if not msg:
            return
        ident = identity(getattr(event, "user", None))
        if not ident:
            return
        raw_id = getattr(event, "msg_id", None) or getattr(event, "message_id", None)
        payload = {**ident, "type": "comment", "comment": msg,
                   "eventId": "c:" + str(raw_id or uuid.uuid4().hex)}
        await relay(payload)

    @client.on(JoinEvent)
    async def on_join(event):
        global last_join_at
        ident = identity(getattr(event, "user", None))
        if not ident:
            return
        now = time.monotonic()
        last_user = join_seen.get(ident["username"], 0)
        if now - last_user < 300 or now - last_join_at < 4:
            return
        join_seen[ident["username"]] = now
        last_join_at = now
        if len(join_seen) > 1500:
            join_seen.clear()
        await relay({**ident, "type": "join", "eventId": "j:" + uuid.uuid4().hex})

    @client.on(GiftEvent)
    async def on_gift(event):
        if getattr(event, "streaking", False):
            return
        ident = identity(getattr(event, "user", None))
        if not ident:
            return
        gift = getattr(event, "gift", None)
        if gift is None:
            return
        gift_name = (getattr(gift, "name", None) or
                     getattr(getattr(gift, "info", None), "name", None) or "هدية")
        gift_id = getattr(event, "gift_id", None) or getattr(gift, "id", "")
        count = max(1, min(int(getattr(event, "repeat_count", 1) or 1), 999))
        raw_id = getattr(event, "msg_id", None) or getattr(event, "message_id", None)
        payload = {**ident, "type": "gift", "giftName": str(gift_name)[:70],
                   "giftId": str(gift_id)[:50], "count": count,
                   "eventId": "g:" + str(raw_id or uuid.uuid4().hex)}
        await relay(payload)

    try:
        is_live = await client.is_live()
    except Exception as exc:
        state["status"] = "checking_error"
        state["last_error"] = type(exc).__name__ + ": " + str(exc)[:100]
        return

    if not is_live:
        state["status"] = "not_live"
        return
    state["status"] = "connecting"
    try:
        await client.connect()
    finally:
        state["room_connected"] = False
        state["status"] = "disconnected"

async def run_forever():
    if not TOKEN:
        state["status"] = "missing_secret"
        state["last_error"] = "FKRAH_INGEST_TOKEN not configured"
        return
    # A synthetic connection check is clearly labelled test, never a real viewer.
    await relay({"type": "test", "source": "test", "eventId": "health:" + uuid.uuid4().hex})
    while True:
        try:
            await run_one()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state["room_connected"] = False
            state["status"] = "retrying"
            state["last_error"] = type(exc).__name__ + ": " + str(exc)[:100]
            log.exception("TikTok LIVE connection problem")
        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(run_forever())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

app = FastAPI(title="FKRAH live collector", lifespan=lifespan, docs_url=None, redoc_url=None)

@app.get("/")
@app.get("/health")
async def health():
    return {"service": "fkrah-tiktok-collector", **state}
