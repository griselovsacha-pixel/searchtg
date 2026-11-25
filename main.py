import os, hmac, hashlib, asyncio, functools
from contextlib import asynccontextmanager
import aiohttp
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import db as D, core as C

PWD = os.environ.get("ADMIN_PASSWORD", "admin")
TOKEN = hmac.new(PWD.encode(), b"admin", hashlib.sha256).hexdigest()
HERE = os.path.dirname(__file__)

async def selfping():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    while url:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as s: await s.get(url + "/health")
        except Exception: pass

@asynccontextmanager
async def life(app):
    app.state.db = await D.init()
    C.ACC = C.Accounts(app.state.db)
    tasks = [asyncio.create_task(C.worker(app.state.db)), asyncio.create_task(C.discoverer(app.state.db)), asyncio.create_task(selfping())]
    yield
    for t in tasks: t.cancel()

app = FastAPI(lifespan=life)

def admin(req: Request):
    if not hmac.compare_digest(req.cookies.get("t", ""), TOKEN): raise HTTPException(401, "Нужен вход")

@app.get("/health")
async def health(): return {"ok": True}

@app.get("/")
async def index(): return FileResponse(os.path.join(HERE, "static/index.html"))

@app.get("/api/chats")
async def chats(req: Request, q: str = "", sort: str = "members", vz: int = 0, limit: int = 48, offset: int = 0):
    order = {"members": "c.members DESC NULLS LAST", "new": "c.found_at DESC", "seen": "c.seen DESC, c.members DESC NULLS LAST",
             "growth": "growth DESC, c.members DESC NULLS LAST"}.get(sort, "c.members DESC NULLS LAST")
    rows = await req.app.state.db.fetch(f"""SELECT c.id,c.title,c.link,c.username,c.members,c.about,c.kind,c.vz,c.seen,c.found_at,
        COALESCE(c.members-(SELECT s.members FROM snapshots s WHERE s.chat_id=c.id AND s.at>now()-interval '7 days' ORDER BY s.at LIMIT 1),0) growth
        FROM chats c WHERE c.title IS NOT NULL AND c.status NOT IN ('dead','skip') AND ($4=0 OR c.vz)
        AND ($1='' OR c.title ILIKE '%'||$1||'%' OR c.about ILIKE '%'||$1||'%') ORDER BY {order} LIMIT $2 OFFSET $3""", q, min(limit, 100), offset, vz)
    return [dict(r) for r in rows]

@app.get("/api/stats")
async def stats(req: Request):
    r = await req.app.state.db.fetchrow("SELECT count(*) FILTER (WHERE title IS NOT NULL) chats, COALESCE(sum(members),0) members, count(*) FILTER (WHERE status='new') queue FROM chats")
    return dict(r)

@app.post("/api/login")
async def login(req: Request, resp: Response):
    if not hmac.compare_digest((await req.json()).get("password", ""), PWD): raise HTTPException(403, "Неверный пароль")
    resp.set_cookie("t", TOKEN, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 30)
    return {"ok": True}

@app.get("/api/admin/overview", dependencies=[Depends(admin)])
async def overview(req: Request):
    db = req.app.state.db
    return {"counts": {r["status"]: r["n"] for r in await db.fetch("SELECT status,count(*) n FROM chats GROUP BY status")},
            "sessions": [dict(r) for r in await db.fetch("SELECT id,label,phone,status,flood_until FROM sessions ORDER BY id")],
            "settings": {k: v for k, v in (await D.settings(db)).items()}, "log": list(C.LOG)}

@app.get("/api/admin/chats", dependencies=[Depends(admin)])
async def admin_chats(req: Request, status: str = "", q: str = ""):
    rows = await req.app.state.db.fetch("""SELECT id,title,link,members,kind,status,error,manual,vz,seen,msgs,cursor,last_msg_id,backfilled,found_at,last_scanned_at FROM chats
        WHERE ($1='' OR status=$1) AND ($2='' OR title ILIKE '%'||$2||'%' OR link ILIKE '%'||$2||'%') ORDER BY found_at DESC LIMIT 300""", status, q)
    return [dict(r) for r in rows]

@app.post("/api/admin/chats", dependencies=[Depends(admin)])
async def add_chats(req: Request):
    text = (await req.json()).get("text", ""); found = C.parse(text)
    if not found: raise HTTPException(400, "Ссылок t.me не найдено")
    new = sum([await C.add(req.app.state.db, k, v, manual=True) for k, v in found])
    return {"found": len(found), "new": new}

@app.post("/api/admin/discover", dependencies=[Depends(admin)])
async def discover_now():
    C.DISCOVER.set(); return {"ok": True}

@app.post("/api/admin/chats/{cid}/rescan", dependencies=[Depends(admin)])
async def rescan(req: Request, cid: int):
    await req.app.state.db.execute("UPDATE chats SET status='new',error=NULL WHERE id=$1 AND status<>'scanning'", cid); return {"ok": True}

@app.delete("/api/admin/chats/{cid}", dependencies=[Depends(admin)])
async def del_chat(req: Request, cid: int):
    await req.app.state.db.execute("DELETE FROM chats WHERE id=$1", cid); return {"ok": True}

def guard(fn):
    @functools.wraps(fn)
    async def w(req: Request, *a, **k):
        try: return await fn(req, *a, **k)
        except HTTPException: raise
        except Exception as e: raise HTTPException(400, str(e) or type(e).__name__)
    return w

@app.post("/api/admin/session/code", dependencies=[Depends(admin)])
@guard
async def s_code(req: Request):
    await C.send_code(req.app.state.db, (await req.json())["phone"].strip()); return {"ok": True}

@app.post("/api/admin/session/login", dependencies=[Depends(admin)])
@guard
async def s_login(req: Request):
    b = await req.json()
    return {"result": await C.sign_in(req.app.state.db, b["phone"].strip(), b["code"].strip(), b.get("password"), b.get("label"))}

@app.post("/api/admin/session/import", dependencies=[Depends(admin)])
@guard
async def s_import(req: Request):
    b = await req.json(); return {"result": await C.import_session(req.app.state.db, b["string_session"], b.get("label"))}

@app.delete("/api/admin/session/{sid}", dependencies=[Depends(admin)])
async def s_del(req: Request, sid: int):
    await req.app.state.db.execute("DELETE FROM sessions WHERE id=$1", sid)
    if C.ACC: await C.ACC.drop(sid)
    return {"ok": True}

@app.put("/api/admin/settings", dependencies=[Depends(admin)])
async def put_settings(req: Request):
    for k, v in (await req.json()).items():
        if k in D.DEFAULTS: await req.app.state.db.execute("UPDATE settings SET value=$2 WHERE key=$1", k, str(v))
    return {"ok": True}
