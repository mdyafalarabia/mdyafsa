"""FKRAH TikTok LIVE relay for @fkrahchannel."""
import asyncio, json, logging, os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from fastapi import FastAPI
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent

CHANNEL = "fkrahchannel"
RELAY_URL = os.environ.get("FKRAH_RELAY_URL","https://fkrah-live-events.mdyafalarabia.workers.dev/ingest")
TOKEN = os.environ.get("FKRAH_INGEST_TOKEN","").strip()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fkrah.collector")
state = dict(channel=CHANNEL,status="initializing",room_connected=False,events_sent=0,last_attempt_at=None,last_connect_at=None,last_relay_at=None,last_error=None)
def now(): return datetime.now(timezone.utc).isoformat()
def avatar_url(user):
    for name in ("avatar_thumb","avatar_medium","avatar_large"):
        item=getattr(user,name,None)
        for url in (getattr(item,"url_list",None) or []):
            if isinstance(url,str) and url.startswith("https://"): return url[:600]
    return ""
def send_sync(data):
    request=Request(RELAY_URL,data=json.dumps(data,ensure_ascii=False).encode(),method="POST",headers={"Content-Type":"application/json","Authorization":"Bearer "+TOKEN})
    with urlopen(request,timeout=12) as response:
        result=json.load(response)
        if not result.get("ok"): raise RuntimeError("relay rejected event")
async def run_one():
    state["status"]="checking_room";state["last_attempt_at"]=now();state["room_connected"]=False
    client=TikTokLiveClient(unique_id="@"+CHANNEL)
    @client.on(ConnectEvent)
    async def connected(event):
        state["status"]="connected";state["room_connected"]=True;state["last_connect_at"]=now()
    @client.on(CommentEvent)
    async def comment(event):
        msg=str(getattr(event,"comment","") or "").strip()
        if msg!="مشاركة" and msg not in {"1","2","3","4","١","٢","٣","٤"} and not msg.startswith("سؤال"):return
        user=getattr(event,"user",None)
        if not user:return
        username=str(getattr(user,"unique_id","") or "").lstrip("@")[:60]
        if not username:return
        data=dict(channel=CHANNEL,source="tiktok",type="comment",username=username,nickname=str(getattr(user,"nickname","") or username)[:70],avatar=avatar_url(user),comment=msg[:240])
        try:
            await asyncio.to_thread(send_sync,data)
            state["events_sent"]+=1;state["last_relay_at"]=now();state["last_error"]=None
        except Exception as e:
            state["last_error"]="relay: "+type(e).__name__;log.exception("Relay failed")
    if not await client.is_live():
        state["status"]="not_live";return
    state["status"]="connecting"
    try: await client.connect()
    finally: state["room_connected"]=False;state["status"]="disconnected"
async def loop():
    if not TOKEN:
        state["status"]="missing_secret";state["last_error"]="FKRAH_INGEST_TOKEN not configured";return
    while True:
        try: await run_one()
        except asyncio.CancelledError:raise
        except Exception as e:
            state["status"]="retrying";state["last_error"]=type(e).__name__+": "+str(e)[:130];log.exception("Connection failed")
        await asyncio.sleep(30)
@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(loop())
    try:yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):await task
app=FastAPI(title="FKRAH TikTok LIVE collector",lifespan=lifespan,docs_url=None,redoc_url=None)
@app.get("/")
@app.get("/health")
async def health():return {"service":"fkrah-tiktok-collector",**state}
