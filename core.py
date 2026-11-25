import asyncio, re, os, collections, datetime as dt
import asyncpg
from telethon import TelegramClient, errors, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, User, ChatInviteAlready, ChannelForbidden, ChatForbidden
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.channels import GetFullChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from db import settings

LOG = collections.deque(maxlen=150)
def log(m): LOG.appendleft(f"{dt.datetime.now():%H:%M:%S}  {m}")

# ---------- поиск ссылок ----------
LINK = re.compile(r"(?:https?://)?(?:t|telegram)\.me/(?:s/)?(\+[\w-]{8,}|joinchat/[\w-]+|[A-Za-z]\w{3,31})(?!\w)", re.I)
TG = re.compile(r"tg://(?:resolve\?domain=([A-Za-z]\w{3,31})|join\?invite=([\w-]+))", re.I)
MENTION = re.compile(r"(?<![\w@/.])@([A-Za-z]\w{4,31})(?!\w)")
SKIP = {"addstickers", "share", "proxy", "socks", "setlanguage", "iv", "addtheme", "boost", "login", "bg", "addemoji", "addlist", "joinchat"}

def ok_name(n): return n.lower() not in SKIP and not n.lower().endswith("bot")

def parse(text):
    out = set(); text = text or ""
    for h in LINK.findall(text):
        if h.lower().startswith("joinchat/"): out.add(("inv", h[9:]))
        elif h.startswith("+"): out.add(("inv", h[1:]))
        elif ok_name(h): out.add(("usr", h.lower()))
    for d, i in TG.findall(text):
        if i: out.add(("inv", i))
        elif ok_name(d): out.add(("usr", d.lower()))
    return out

def texts(m):
    """Весь текст сообщения: подпись, скрытые ссылки, кнопки, превью ссылки."""
    t = [m.raw_text or ""]
    for e in m.entities or []:
        if getattr(e, "url", None): t.append(e.url)
    for r in (m.buttons or []):
        for b in r:
            if getattr(b, "url", None): t.append(b.url)
    wp = getattr(m.media, "webpage", None)
    if wp is not None and getattr(wp, "url", None): t.append(wp.url)
    return " ".join(t)

def harvest(m, found, kw, vzr, mode, need):
    txt = texts(m)
    vz = bool(vzr.search(txt)); hit = vz or bool(kw.search(txt))
    if need and not hit: return
    links = parse(txt)
    if mode == "1" or (mode == "kw" and hit):
        links |= {("usr", u.lower()) for u in MENTION.findall(txt) if ok_name(u)}
    fw = getattr(m, "forward", None)
    u = getattr(getattr(fw, "chat", None), "username", None) if fw else None
    if u and ok_name(u): links.add(("usr", u.lower()))
    for k in links:
        f = found.setdefault(k, [0, False]); f[0] += 1; f[1] = f[1] or vz

async def add(db, kind, val, src=None, manual=False, n=1, vz=False):
    """True, если чат новый. Известный чат: +упоминания; при ручном добавлении ставится на пересканирование."""
    col, link = ("invite_hash", f"https://t.me/+{val}") if kind == "inv" else ("username", f"https://t.me/{val}")
    r = await db.execute(f"INSERT INTO chats({col},link,source_id,manual,seen,vz) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", val, link, src, manual, n, vz)
    if r.endswith(" 1"): return True
    await db.execute(f"UPDATE chats SET seen=seen+$2, vz=vz OR $3, status=CASE WHEN $4 AND status<>'scanning' THEN 'new' ELSE status END WHERE {col}=$1", val, n, vz, manual)
    return False

# ---------- аккаунты ----------
async def creds(db):
    S = await settings(db)
    i, h = S["api_id"] or os.environ.get("API_ID"), S["api_hash"] or os.environ.get("API_HASH")
    if not i or not h: raise ValueError("Укажите API ID и API Hash в настройках")
    return int(i), h

class Accounts:
    def __init__(s, db): s.db, s.clients, s.i = db, {}, 0
    async def get(s, prefer=None, locked=False):
        rows = await s.db.fetch("SELECT id,string_session FROM sessions WHERE status='active' AND (flood_until IS NULL OR flood_until<now()) ORDER BY id")
        if prefer:
            mine = [r for r in rows if r["id"] == prefer]
            if mine: rows = mine
            elif locked: return "wait"      # чат читается только тем аккаунтом, что в него вступил
        if not rows: return None
        r = rows[s.i % len(rows)]; s.i += 1
        cl = s.clients.get(r["id"])
        if not cl:
            api_id, api_hash = await creds(s.db)
            cl = TelegramClient(StringSession(r["string_session"]), api_id, api_hash)
            await cl.connect()
            if not await cl.is_user_authorized():
                await s.db.execute("UPDATE sessions SET status='dead' WHERE id=$1", r["id"]); return None
            s.clients[r["id"]] = cl
        return r["id"], cl
    async def drop(s, sid):
        cl = s.clients.pop(sid, None)
        if cl: await cl.disconnect()

ACC = None

# ---------- сканирование ----------
async def resolve(db, cl, row, S):
    """-> (чат, вступили_сейчас). (None, False) — чат уже обработан здесь."""
    if row["username"]:
        return await cl.get_entity(row["username"]), False
    r = await cl(CheckChatInviteRequest(row["invite_hash"]))
    if isinstance(r, ChatInviteAlready): return r.chat, False
    if S["join_private"] != "1":
        await db.execute("UPDATE chats SET title=$2,about=$3,members=$4,kind='private',status='private',error=NULL,last_scanned_at=now() WHERE id=$1",
                         row["id"], r.title, getattr(r, "about", None), getattr(r, "participants_count", None))
        return None, False
    return (await cl(ImportChatInviteRequest(row["invite_hash"]))).chats[0], True

async def read(cl, ent, row, size, found, kw, vzr, mode, need):
    """Одна порция истории. Большие чаты читаются от новых сообщений к старым, курсор хранится в БД."""
    n, mn, mx, flood = 0, None, 0, None
    if row["backfilled"]: it = cl.iter_messages(ent, limit=None, min_id=row["last_msg_id"] or 0, wait_time=1)
    else: it = cl.iter_messages(ent, limit=size, offset_id=row["cursor"] or 0, wait_time=1)
    try:
        async for m in it:
            n += 1; mn = m.id if mn is None else min(mn, m.id); mx = max(mx, m.id)
            harvest(m, found, kw, vzr, mode, need)
    except errors.FloodWaitError as e:
        flood = e
    return n, mn, mx, flood

async def scan(db, cl, sid, row, S):
    ent, joined = await resolve(db, cl, row, S)
    if ent is None: return
    if isinstance(ent, User):
        await db.execute("UPDATE chats SET status='skip',error='это бот/пользователь' WHERE id=$1", row["id"]); return
    if isinstance(ent, (ChannelForbidden, ChatForbidden)):
        await db.execute("UPDATE chats SET status='dead',error='нет доступа' WHERE id=$1", row["id"]); return
    title, about, kind = ent.title, None, "group"
    members = getattr(ent, "participants_count", None)
    if isinstance(ent, Channel):
        f = (await cl(GetFullChannelRequest(ent))).full_chat
        members, about = f.participants_count, f.about
        kind = "channel" if ent.broadcast else ("forum" if getattr(ent, "forum", False) else "group")
    uname = (getattr(ent, "username", None) or "").lower() or None
    link = f"https://t.me/{uname}" if uname else row["link"]
    kw, vzr = re.compile(S["kw_regex"], re.I), re.compile(S["vz_regex"], re.I)
    size, found = int(S["slice"]), {}
    args = (cl, ent, row, size, found, kw, vzr, S["mentions"], S["require_kw"] == "1")
    try:
        n, mn, mx, flood = await read(*args)
    except errors.ChannelPrivateError:      # читать можно только участнику — вступаем
        if joined or row["locked"] or S["join_private"] != "1": raise
        await cl(JoinChannelRequest(ent)); joined = True; found.clear()
        n, mn, mx, flood = await read(*args)
    found.pop(("usr", uname), None); found.pop(("inv", row["invite_hash"]), None)
    new = 0
    for (k, v), (cnt, vz) in found.items():
        if await add(db, k, v, src=row["id"], n=cnt, vz=vz): new += 1
    if row["backfilled"]:
        if flood: log(f"FloodWait при чтении {title}"); raise flood   # пробел в новых сообщениях не пропускаем
        cursor, last, backfilled = row["cursor"], max(row["last_msg_id"] or 0, mx), True
    else:
        cursor = mn or row["cursor"]; last = mx if not row["cursor"] else row["last_msg_id"]
        backfilled = n < size and not flood
    ours = joined or row["locked"]
    leave = ours and backfilled and S["leave_after"] == "1"
    if leave:
        try: await cl.delete_dialog(ent)
        except Exception: leave = False
    vz_chat = bool(vzr.search(f"{title} {about or ''}"))
    try:
        await db.execute("""UPDATE chats SET tg_id=$2,title=$3,about=$4,members=$5,kind=$6,link=$7,username=COALESCE(username,$8::text),
            status=$9,error=NULL,cursor=$10,last_msg_id=$11,backfilled=$12,msgs=msgs+$13,vz=vz OR $14,acc=$15,locked=$16,last_scanned_at=now() WHERE id=$1""",
            row["id"], utils.get_peer_id(ent), title, about, members, kind, link, uname,
            "done" if backfilled else "new", cursor or 0, last or 0, backfilled, n, vz_chat, sid, ours and not leave)
    except asyncpg.UniqueViolationError:
        await db.execute("DELETE FROM chats WHERE id=$1", row["id"]); log(f"дубль: {title}"); return
    if members is not None:
        await db.execute("INSERT INTO snapshots(chat_id,members) SELECT $1,$2 WHERE NOT EXISTS (SELECT 1 FROM snapshots WHERE chat_id=$1 AND at>now()-interval '6 hours')", row["id"], members)
    log(f"✓ {title} · {members or '?'} уч. · +{n} сообщ. · {'история готова' if backfilled else f'дошли до id {cursor}'} · новых чатов: {new}")
    if flood: raise flood

async def process(db, row, S):
    try: got = await ACC.get(row["acc"], row["locked"])
    except Exception as e: log(f"аккаунты: {e}"); got = None
    requeue = "UPDATE chats SET status='new',last_scanned_at=now() WHERE id=$1"
    if got == "wait":
        await db.execute(requeue, row["id"]); log(f"{row['title']}: ждём аккаунт #{row['acc']}"); return False
    if not got:
        await db.execute(requeue, row["id"]); log("нет доступных аккаунтов — добавьте сессию или дождитесь окончания flood"); return False
    sid, cl = got
    try:
        await scan(db, cl, sid, row, S)
    except (errors.FloodWaitError, errors.ChannelsTooMuchError) as e:
        sec = getattr(e, "seconds", 3600) + 5
        await db.execute("UPDATE sessions SET flood_until=now()+$2::int*interval '1 second' WHERE id=$1", sid, sec)
        await db.execute(requeue, row["id"])
        log(f"аккаунт #{sid} отдыхает {sec}с ({type(e).__name__})"); return False
    except (errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.UserDeactivatedBanError, errors.SessionRevokedError):
        await db.execute("UPDATE sessions SET status='dead' WHERE id=$1", sid); await ACC.drop(sid)
        await db.execute(requeue, row["id"]); log(f"аккаунт #{sid} недействителен")
    except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError, errors.InviteHashExpiredError, errors.InviteHashInvalidError,
            errors.ChannelPrivateError, errors.InviteRequestSentError, errors.UserBannedInChannelError, ValueError) as e:
        await db.execute("UPDATE chats SET status='dead',error=$2 WHERE id=$1", row["id"], type(e).__name__)
    except Exception as e:
        await db.execute("UPDATE chats SET status='error',error=$2 WHERE id=$1", row["id"], str(e)[:200]); log(f"ошибка: {e}")
    return True

async def worker(db):
    global ACC
    ACC = ACC or Accounts(db); log("воркер запущен")
    while True:
        try:
            S = await settings(db)
            row = await db.fetchrow("""UPDATE chats SET status='scanning' WHERE id=(SELECT id FROM chats
                WHERE status='new' OR (status IN ('done','private') AND last_scanned_at < now() - $1::int*interval '1 hour')
                ORDER BY (status='new') DESC, last_scanned_at NULLS FIRST, found_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *""", int(S["rescan_hours"]))
            if not row: await asyncio.sleep(20); continue
            ok = await process(db, row, S)
            await asyncio.sleep(int(S["delay"]) if ok else 30)
        except asyncio.CancelledError: raise
        except Exception as e:
            log(f"сбой цикла: {e}"); await asyncio.sleep(30)

# ---------- активный поиск новых чатов ----------
DISCOVER = asyncio.Event()   # админка: «искать сейчас»

async def discover(db, S):
    """1) публичные чаты и каналы по названию; 2) глобальный поиск по постам — из них берутся
    открытые и закрытые (t.me/+...) ссылки. Всё найденное попадает в очередь на сканирование."""
    got = await ACC.get()
    if not got or got == "wait":
        log("поиск: нет доступных аккаунтов"); return False
    sid, cl = got
    kw, vzr = re.compile(S["kw_regex"], re.I), re.compile(S["vz_regex"], re.I)
    found = {}
    try:
        for q in [x.strip() for x in S["search_queries"].splitlines() if x.strip()]:
            for ch in (await cl(SearchRequest(q=q, limit=100))).chats:
                u = getattr(ch, "username", None)
                if u and ok_name(u):
                    f = found.setdefault(("usr", u.lower()), [0, False]); f[0] += 1; f[1] = f[1] or bool(vzr.search(getattr(ch, "title", "") or ""))
            async for m in cl.iter_messages(None, search=q, limit=int(S["search_limit"]), wait_time=2):
                harvest(m, found, kw, vzr, S["mentions"], False)
            await asyncio.sleep(3)
    except errors.FloodWaitError as e:
        await db.execute("UPDATE sessions SET flood_until=now()+$2::int*interval '1 second' WHERE id=$1", sid, e.seconds + 5)
        log(f"поиск: аккаунт #{sid} отдыхает {e.seconds}с")
    new = 0
    for (k, v), (cnt, vz) in found.items():
        if await add(db, k, v, n=cnt, vz=vz): new += 1
    log(f"поиск: ссылок найдено {len(found)}, новых чатов {new}")
    return True

async def discoverer(db):
    while ACC is None: await asyncio.sleep(1)
    await asyncio.sleep(60)
    while True:
        try:
            S = await settings(db); DISCOVER.clear()
            if not await discover(db, S): await asyncio.sleep(60); continue
            try: await asyncio.wait_for(DISCOVER.wait(), timeout=float(S["search_hours"]) * 3600)
            except asyncio.TimeoutError: pass
        except asyncio.CancelledError: raise
        except Exception as e:
            log(f"поиск, сбой: {e}"); await asyncio.sleep(300)

# ---------- вход в аккаунт через сайт ----------
PENDING = {}

async def send_code(db, phone):
    api_id, api_hash = await creds(db)
    if phone in PENDING: await PENDING.pop(phone)[0].disconnect()
    cl = TelegramClient(StringSession(), api_id, api_hash); await cl.connect()
    s = await cl.send_code_request(phone)
    PENDING[phone] = (cl, s.phone_code_hash)

async def sign_in(db, phone, code, pwd, label):
    cl, h = PENDING[phone]
    try:
        await cl.sign_in(phone, code, phone_code_hash=h)
    except errors.SessionPasswordNeededError:
        if not pwd: return "need_password"
        await cl.sign_in(password=pwd)
    PENDING.pop(phone, None)
    return await save(db, cl, phone, label)

async def save(db, cl, phone, label):
    me = await cl.get_me(); ss = cl.session.save(); await cl.disconnect()
    await db.execute("INSERT INTO sessions(label,phone,string_session) VALUES($1,$2,$3)", label or me.first_name, phone or me.phone, ss)
    return "ok"

async def import_session(db, string, label):
    api_id, api_hash = await creds(db)
    cl = TelegramClient(StringSession(string.strip()), api_id, api_hash); await cl.connect()
    if not await cl.is_user_authorized(): await cl.disconnect(); raise ValueError("Сессия не авторизована")
    return await save(db, cl, None, label)
