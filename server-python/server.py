"""
BBS Server (Python) - Conecta ao broker, processa login/canais e persiste em SQLite.
"""

import os
import time
import sqlite3
import zmq
import msgpack


BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "5556"))
SERVER_ID = os.environ.get("SERVER_ID", "python-server-1")
DB_PATH = os.environ.get("DB_PATH", f"/data/{SERVER_ID}.db")


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            username  TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name      TEXT PRIMARY KEY,
            created_by TEXT NOT NULL,
            timestamp  INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn


def save_login(conn: sqlite3.Connection, username: str, ts: int):
    conn.execute("INSERT INTO logins (username, timestamp) VALUES (?, ?)", (username, ts))
    conn.commit()


def channel_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM channels WHERE name = ?", (name,)).fetchone()
    return row is not None


def save_channel(conn: sqlite3.Connection, name: str, created_by: str, ts: int):
    conn.execute(
        "INSERT INTO channels (name, created_by, timestamp) VALUES (?, ?, ?)",
        (name, created_by, ts),
    )
    conn.commit()


def list_channels(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM channels ORDER BY name").fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Protocolo
# ---------------------------------------------------------------------------

def make_response(msg_type: str, payload: dict) -> bytes:
    return msgpack.packb({
        "type": msg_type,
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }, use_bin_type=True)


def parse_message(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

logged_in_users: set[str] = set()


def handle_login(conn, payload: dict, ts: int) -> bytes:
    username = payload.get("username", "").strip()
    if not username:
        log("LOGIN_ERROR: username vazio")
        return make_response("LOGIN_ERROR", {"username": "", "reason": "invalid_username"})
    if username in logged_in_users:
        log(f"LOGIN_ERROR: '{username}' já está logado")
        return make_response("LOGIN_ERROR", {"username": username, "reason": "already_logged_in"})

    logged_in_users.add(username)
    save_login(conn, username, ts)
    log(f"LOGIN_OK: '{username}' | timestamp={ts}")
    return make_response("LOGIN_OK", {"username": username})


def handle_list_channels(conn, payload: dict) -> bytes:
    channels = list_channels(conn)
    log(f"LIST_CHANNELS: retornando {len(channels)} canal(is) → {channels}")
    return make_response("CHANNEL_LIST", {"channels": channels})


def handle_create_channel(conn, payload: dict, ts: int) -> bytes:
    channel = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")

    if not channel or " " in channel:
        log(f"CHANNEL_ERROR: nome inválido '{channel}'")
        return make_response("CHANNEL_ERROR", {"reason": "invalid_channel_name"})

    if channel_exists(conn, channel):
        log(f"CHANNEL_EXISTS: '{channel}' já existe")
        return make_response("CHANNEL_EXISTS", {"channel": channel})

    save_channel(conn, channel, username, ts)
    log(f"CHANNEL_CREATED: '{channel}' por '{username}' | timestamp={ts}")
    return make_response("CHANNEL_CREATED", {"channel": channel})


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{SERVER_ID} {ts}] {msg}", flush=True)


def main():
    conn = init_db(DB_PATH)
    log(f"Banco de dados inicializado em '{DB_PATH}'")

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    addr = f"tcp://{BROKER_HOST}:{BROKER_PORT}"
    socket.connect(addr)
    log(f"Conectado ao broker em {addr}. Aguardando mensagens...")

    while True:
        try:
            raw = socket.recv()
            msg = parse_message(raw)
            msg_type = msg.get("type", "")
            payload = msg.get("payload", {})
            ts = msg.get("timestamp", int(time.time() * 1000))

            log(f"RECEBIDO | type={msg_type} | payload={payload} | timestamp={ts}")

            if msg_type == "LOGIN":
                response = handle_login(conn, payload, ts)
            elif msg_type == "LIST_CHANNELS":
                response = handle_list_channels(conn, payload)
            elif msg_type == "CREATE_CHANNEL":
                response = handle_create_channel(conn, payload, ts)
            else:
                log(f"TIPO DESCONHECIDO: '{msg_type}'")
                response = make_response("ERROR", {"reason": f"unknown_type: {msg_type}"})

            resp_parsed = msgpack.unpackb(response, raw=False)
            log(f"ENVIANDO | type={resp_parsed['type']} | payload={resp_parsed['payload']}")
            socket.send(response)

        except Exception as e:
            log(f"ERRO: {e}")
            error_resp = make_response("ERROR", {"reason": str(e)})
            socket.send(error_resp)


if __name__ == "__main__":
    main()
