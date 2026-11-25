import os, asyncpg

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings(key text PRIMARY KEY, value text NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(id serial PRIMARY KEY, label text, phone text, string_session text NOT NULL,
  status text DEFAULT 'active', flood_until timestamptz, created_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS chats(id serial PRIMARY KEY, tg_id bigint UNIQUE, username text UNIQUE, invite_hash text UNIQUE,
  title text, link text, about text, members int, kind text, status text DEFAULT 'new', error text, source_id int,
  manual bool DEFAULT false, last_msg_id bigint DEFAULT 0, last_scanned_at timestamptz, found_at timestamptz DEFAULT now());
ALTER TABLE chats ADD COLUMN IF NOT EXISTS cursor bigint DEFAULT 0;        -- до какого id дочитана история (вглубь)
ALTER TABLE chats ADD COLUMN IF NOT EXISTS backfilled bool DEFAULT false;  -- вся история прочитана
ALTER TABLE chats ADD COLUMN IF NOT EXISTS msgs bigint DEFAULT 0;          -- сколько сообщений обработано
ALTER TABLE chats ADD COLUMN IF NOT EXISTS seen int DEFAULT 1;             -- сколько раз ссылку упоминали
ALTER TABLE chats ADD COLUMN IF NOT EXISTS vz bool DEFAULT false;          -- чат относится к ВЗ
ALTER TABLE chats ADD COLUMN IF NOT EXISTS acc int;                        -- аккаунт, который читает чат
ALTER TABLE chats ADD COLUMN IF NOT EXISTS locked bool DEFAULT false;      -- вступили по инвайту, читать можно только этим аккаунтом
CREATE TABLE IF NOT EXISTS snapshots(id serial PRIMARY KEY, chat_id int REFERENCES chats(id) ON DELETE CASCADE,
  members int, at timestamptz DEFAULT now());
CREATE INDEX IF NOT EXISTS ix_chats_status ON chats(status);
CREATE INDEX IF NOT EXISTS ix_snap ON snapshots(chat_id, at);
"""

DEFAULTS = {
    "api_id": "", "api_hash": "",
    # строгий фильтр: чат/сообщение относится к взаимным подпискам (ставит метку «ВЗ»)
    "vz_regex": r"\bвз\b|в/з|\bвзаим|\bвп\b|обмен\w*\s+(подпис|рефер|чат|трафик)",
    # широкий фильтр: из таких сообщений дополнительно берутся @упоминания
    "kw_regex": r"\bвз\b|в/з|\bвп\b|взаим|обмен|подпис|пиар|рефер|чат|групп|канал|конкурс|розыгр|набор",
    "require_kw": "0",      # 1 = брать ссылки только из сообщений с ключевыми словами; 0 = брать все t.me-ссылки
    "mentions": "kw",       # @упоминания: 0 — не брать, kw — из сообщений по широкому фильтру, 1 — из всех
    "join_private": "1",    # вступать в закрытые чаты (и в те, где без вступления история недоступна)
    "leave_after": "1",     # выходить из чата, когда история прочитана (чтобы не упереться в лимит чатов)
    "slice": "5000",        # сообщений за один заход (история больших чатов читается кусками)
    "rescan_hours": "24",
    "search_queries": "взаимные подписки\nвз чат\nвз чатик\nвзаимка\nвзаимопиар\nвзаимный пиар\nобмен подписками\nвзаимные рефералы\nвз рефералы\nобмен рефералами\nвзаимная подписка\nвзаємні підписки\nвп чат",
    "search_limit": "300",  # сообщений на запрос в глобальном поиске по постам
    "search_hours": "12",   # автопоиск новых чатов раз в N часов
    "delay": "10",          # пауза между заходами, сек
}

async def init():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=4, statement_cache_size=0)
    async with pool.acquire() as c:
        await c.execute(SCHEMA)
        for t in ("settings", "sessions", "chats", "snapshots"):
            await c.execute(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")  # без политик: доступ только у бэкенда
        for k, v in DEFAULTS.items():
            await c.execute("INSERT INTO settings VALUES($1,$2) ON CONFLICT DO NOTHING", k, v)
        await c.execute("UPDATE chats SET status='new' WHERE status='scanning'")
    return pool

async def settings(db):
    return {r["key"]: r["value"] for r in await db.fetch("SELECT key,value FROM settings")}
