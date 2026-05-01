"""
BBS Server (Python) - Parte 4
Novidades:
  - Eleição pelo algoritmo de Bully (menor rank ganha)
  - Sincronização do relógio via algoritmo de Berkeley (coordenador → servidor)
  - Heartbeat não retorna mais hora (removido da referência)
  - Cada servidor expõe porta REP dedicada para comunicação servidor↔servidor
  - Coordenador eleito é anunciado no tópico PUB/SUB 'servers'
"""

import os
import time
import sqlite3
import threading
import zmq
import msgpack

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BROKER_HOST     = os.environ.get("BROKER_HOST",     "broker")
BROKER_PORT     = int(os.environ.get("BROKER_PORT",     "5556"))
PROXY_HOST      = os.environ.get("PROXY_HOST",      "proxy-pubsub")
PROXY_PORT      = int(os.environ.get("PROXY_PORT",      "5557"))
PROXY_SUB_PORT  = int(os.environ.get("PROXY_SUB_PORT",  "5558"))
REF_HOST        = os.environ.get("REF_HOST",        "reference")
REF_PORT        = int(os.environ.get("REF_PORT",        "5560"))
SERVER_ID       = os.environ.get("SERVER_ID",       "python-server-1")
DB_PATH         = os.environ.get("DB_PATH",         f"/data/bbs.db")

# Porta REP deste servidor para comunicação servidor↔servidor
# Cada container expõe a sua própria porta interna 6000
S2S_PORT        = int(os.environ.get("S2S_PORT", "6000"))

# Mapeamento nome→endereço para comunicação direta servidor↔servidor
# Injetado via variável de ambiente como "nome1=host1:porta1,nome2=host2:porta2"
S2S_PEERS_ENV   = os.environ.get("S2S_PEERS", "")

HEARTBEAT_EVERY = 10    # heartbeat na referência
SYNC_EVERY      = 15    # sincronização de relógio com o coordenador
ELECTION_TIMEOUT_MS = 2000   # timeout para resposta de eleição
CLOCK_TIMEOUT_MS    = 2000   # timeout para resposta de sincronização


# ---------------------------------------------------------------------------
# Parse dos peers servidor↔servidor
# formato: "python-server-2=bbs-server-python-2:6000,js-server-1=bbs-server-js-1:6000,..."
# ---------------------------------------------------------------------------
def parse_peers(env: str) -> dict:
    peers = {}
    for part in env.split(","):
        part = part.strip()
        if "=" in part:
            name, addr = part.split("=", 1)
            peers[name.strip()] = addr.strip()
    return peers


S2S_PEERS: dict = parse_peers(S2S_PEERS_ENV)


# ---------------------------------------------------------------------------
# Relógio lógico de Lamport
# ---------------------------------------------------------------------------
_clock_lock    = threading.Lock()
_logical_clock = 0


def clock_tick() -> int:
    global _logical_clock
    with _clock_lock:
        _logical_clock += 1
        return _logical_clock


def clock_update(received: int):
    global _logical_clock
    with _clock_lock:
        _logical_clock = max(_logical_clock, int(received or 0)) + 1


def clock_get() -> int:
    with _clock_lock:
        return _logical_clock


# ---------------------------------------------------------------------------
# Relógio físico
# ---------------------------------------------------------------------------
_physical_offset_ms = 0


def physical_now_ms() -> int:
    return int(time.time() * 1000) + _physical_offset_ms


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{SERVER_ID} {ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------
_db_lock = threading.Lock()

def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY,
            created_by TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            username TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )""")
    conn.commit()
    return conn


def channel_exists(conn, name) -> bool:
    return conn.execute("SELECT 1 FROM channels WHERE name=?", (name,)).fetchone() is not None


def list_channels(conn) -> list:
    return [r[0] for r in conn.execute("SELECT name FROM channels ORDER BY name").fetchall()]


# ---------------------------------------------------------------------------
# Protocolo helpers
# ---------------------------------------------------------------------------
def pack(msg_type: str, payload: dict) -> bytes:
    lc = clock_tick()
    return msgpack.packb({
        "type": msg_type,
        "timestamp": physical_now_ms(),
        "logical_clock": lc,
        "payload": payload,
    }, use_bin_type=True)


def unpack(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


# ---------------------------------------------------------------------------
# Estado de eleição / coordenador
# ---------------------------------------------------------------------------
_state_lock  = threading.Lock()
_coordinator = None   # nome do coordenador atual
_my_rank     = 0
_election_in_progress = False


def get_coordinator():
    with _state_lock:
        return _coordinator


def set_coordinator(name: str):
    global _coordinator
    with _state_lock:
        _coordinator = name
    log(f"★ Coordenador definido: '{name}'")


def get_rank():
    with _state_lock:
        return _my_rank


def set_rank(r: int):
    global _my_rank
    with _state_lock:
        _my_rank = r


# ---------------------------------------------------------------------------
# Comunicação com o serviço de referência
# ---------------------------------------------------------------------------
_ref_lock   = threading.Lock()
_ref_socket = None


def ref_send_recv(msg_type: str, payload: dict) -> dict:
    lc  = clock_tick()
    raw = msgpack.packb({
        "type": msg_type, "timestamp": physical_now_ms(),
        "logical_clock": lc, "payload": payload,
    }, use_bin_type=True)
    with _ref_lock:
        _ref_socket.send(raw)
        resp = msgpack.unpackb(_ref_socket.recv(), raw=False)
    clock_update(resp.get("logical_clock", 0))
    return resp


def register_with_reference(context):
    global _ref_socket
    _ref_socket = context.socket(zmq.REQ)
    _ref_socket.connect(f"tcp://{REF_HOST}:{REF_PORT}")
    resp = ref_send_recv("GET_RANK", {"name": SERVER_ID})
    rank = resp.get("payload", {}).get("rank", 0)
    set_rank(rank)
    log(f"Registrado na referência | rank={rank}")


def do_heartbeat():
    """Heartbeat para manter presença na referência (sem sincronização de relógio)."""
    try:
        resp = ref_send_recv("HEARTBEAT", {"name": SERVER_ID})
        log(f"HEARTBEAT OK | lc={clock_get()}")
    except Exception as e:
        log(f"HEARTBEAT ERRO: {e}")


def get_server_list() -> list:
    """Obtém lista de servidores da referência: [{name, rank}, ...]"""
    try:
        resp = ref_send_recv("LIST_SERVERS", {})
        return resp.get("payload", {}).get("servers", [])
    except Exception as e:
        log(f"LIST_SERVERS ERRO: {e}")
        return []


# ---------------------------------------------------------------------------
# Comunicação servidor↔servidor (REQ com timeout)
# ---------------------------------------------------------------------------
_zmq_context_global = None


def s2s_request(peer_addr: str, msg_type: str, payload: dict,
                timeout_ms: int = ELECTION_TIMEOUT_MS) -> dict | None:
    """Envia REQ a outro servidor e aguarda resposta com timeout. Retorna None se falhar."""
    ctx  = _zmq_context_global
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.connect(f"tcp://{peer_addr}")
        sock.send(pack(msg_type, payload))
        raw  = sock.recv()
        resp = unpack(raw)
        clock_update(resp.get("logical_clock", 0))
        return resp
    except zmq.Again:
        return None
    except Exception as e:
        log(f"s2s_request ERRO ({peer_addr}): {e}")
        return None
    finally:
        sock.close()


# ---------------------------------------------------------------------------
# Eleição — algoritmo de Bully (menor rank = maior prioridade)
# ---------------------------------------------------------------------------
def start_election(pub_socket):
    global _election_in_progress
    with _state_lock:
        if _election_in_progress:
            return
        _election_in_progress = True

    log(f"ELEIÇÃO iniciada | meu rank={get_rank()}")

    servers      = get_server_list()
    my_rank      = get_rank()

    # Contata apenas servidores com rank MENOR (maior prioridade)
    higher_responded = False
    for srv in servers:
        if srv["name"] == SERVER_ID:
            continue
        if srv["rank"] >= my_rank:
            continue   # só contata quem tem prioridade maior (rank menor)

        peer_addr = S2S_PEERS.get(srv["name"])
        if not peer_addr:
            continue

        log(f"ELEIÇÃO → contactando '{srv['name']}' (rank={srv['rank']})")
        resp = s2s_request(peer_addr, "ELECTION", {"candidate": SERVER_ID})
        if resp and resp.get("type") == "ELECTION_OK":
            log(f"ELEIÇÃO → '{srv['name']}' respondeu OK, ele assumirá")
            higher_responded = True
            break

    if not higher_responded:
        # Ninguém de maior prioridade respondeu → sou o coordenador
        log(f"ELEIÇÃO → ninguém respondeu, sou o coordenador!")
        set_coordinator(SERVER_ID)
        _announce_coordinator(pub_socket)

    with _state_lock:
        _election_in_progress = False


def _announce_coordinator(pub_socket):
    """Publica no tópico 'servers' o nome do novo coordenador."""
    lc = clock_tick()
    payload = msgpack.packb({
        "coordinator": SERVER_ID,
        "timestamp":   physical_now_ms(),
        "logical_clock": lc,
    }, use_bin_type=True)
    pub_socket.send_multipart([b"servers", payload])
    log(f"ANUNCIO coordenador='{SERVER_ID}' publicado no tópico 'servers' | lc={lc}")


# ---------------------------------------------------------------------------
# Sincronização de relógio — algoritmo de Berkeley (simplificado)
# Servidor pede hora ao coordenador; coordenador responde com timestamp
# ---------------------------------------------------------------------------
def sync_clock_with_coordinator(pub_socket):
    coord = get_coordinator()
    if not coord:
        log("SYNC: sem coordenador definido, iniciando eleição...")
        threading.Thread(target=start_election, args=(pub_socket,), daemon=True).start()
        return

    if coord == SERVER_ID:
        log("SYNC: sou o coordenador, relógio já é a referência")
        return

    peer_addr = S2S_PEERS.get(coord)
    if not peer_addr:
        log(f"SYNC: endereço do coordenador '{coord}' desconhecido, iniciando eleição...")
        threading.Thread(target=start_election, args=(pub_socket,), daemon=True).start()
        return

    t_send = int(time.time() * 1000)
    resp   = s2s_request(peer_addr, "GET_CLOCK", {"requester": SERVER_ID},
                         timeout_ms=CLOCK_TIMEOUT_MS)
    t_recv = int(time.time() * 1000)

    if resp is None:
        log(f"SYNC: coordenador '{coord}' não respondeu → iniciando eleição")
        threading.Thread(target=start_election, args=(pub_socket,), daemon=True).start()
        return

    coord_clock = resp.get("payload", {}).get("clock", 0)
    if coord_clock:
        global _physical_offset_ms
        rtt_half = (t_recv - t_send) // 2
        _physical_offset_ms = coord_clock + rtt_half - t_recv
        log(f"SYNC OK | coord='{coord}' | coord_clock={coord_clock} | offset={_physical_offset_ms:+d}ms | lc={clock_get()}")


# ---------------------------------------------------------------------------
# Thread SUB — escuta tópico 'servers' para atualizar coordenador
# ---------------------------------------------------------------------------
def coordinator_subscriber(context):
    sub = context.socket(zmq.SUB)
    sub.connect(f"tcp://{PROXY_HOST}:{PROXY_SUB_PORT}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
    log("SUB inscrito no tópico 'servers'")

    while True:
        try:
            parts   = sub.recv_multipart()
            data    = msgpack.unpackb(parts[1], raw=False)
            coord   = data.get("coordinator", "")
            recv_lc = data.get("logical_clock", 0)
            clock_update(recv_lc)
            if coord:
                set_coordinator(coord)
                log(f"SUB [servers] novo coordenador='{coord}' | lc={clock_get()}")
        except Exception as e:
            log(f"SUB [servers] ERRO: {e}")


# ---------------------------------------------------------------------------
# Thread REP servidor↔servidor (s2s)
# ---------------------------------------------------------------------------
def s2s_server(context, pub_socket):
    rep = context.socket(zmq.REP)
    rep.bind(f"tcp://*:{S2S_PORT}")
    log(f"S2S REP escutando na porta {S2S_PORT}")

    while True:
        try:
            raw     = rep.recv()
            msg     = unpack(raw)
            mtype   = msg.get("type", "")
            payload = msg.get("payload", {})
            recv_lc = msg.get("logical_clock", 0)
            clock_update(recv_lc)

            if mtype == "ELECTION":
                # Recebo eleição de um servidor com rank MAIOR (menor prioridade)
                # Respondo OK e inicio minha própria eleição
                candidate = payload.get("candidate", "?")
                log(f"S2S ELECTION ← '{candidate}' | respondendo OK e iniciando minha eleição")
                rep.send(pack("ELECTION_OK", {}))
                threading.Thread(target=start_election, args=(pub_socket,), daemon=True).start()

            elif mtype == "GET_CLOCK":
                # Coordenador responde com seu timestamp atual
                requester = payload.get("requester", "?")
                now = physical_now_ms()
                log(f"S2S GET_CLOCK ← '{requester}' | respondendo clock={now}")
                rep.send(pack("CLOCK", {"clock": now}))

            else:
                log(f"S2S tipo desconhecido: '{mtype}'")
                rep.send(pack("ERROR", {"reason": f"unknown: {mtype}"}))

        except Exception as e:
            log(f"S2S ERRO: {e}")
            try:
                rep.send(pack("ERROR", {"reason": str(e)}))
            except:
                pass


# ---------------------------------------------------------------------------
# Handlers de mensagem de clientes
# ---------------------------------------------------------------------------
_logged_in_users: set = set()


def handle_login(conn, payload, ts, recv_lc):
    clock_update(recv_lc)
    username = payload.get("username", "").strip()
    if not username:
        return pack("LOGIN_ERROR", {"username": "", "reason": "invalid_username"})
    if username in _logged_in_users:
        return pack("LOGIN_ERROR", {"username": username, "reason": "already_logged_in"})
    _logged_in_users.add(username)
    lc = clock_get()
    with _db_lock:
        conn.execute("INSERT INTO logins (username, timestamp, logical_clock) VALUES (?,?,?)",
                     (username, ts, lc))
        conn.commit()
    log(f"LOGIN_OK: '{username}' | lc={lc}")
    return pack("LOGIN_OK", {"username": username})


def handle_list_channels(conn, payload, recv_lc):
    clock_update(recv_lc)
    channels = list_channels(conn)
    log(f"LIST_CHANNELS: {len(channels)} canal(is) | lc={clock_get()}")
    return pack("CHANNEL_LIST", {"channels": channels})


def handle_create_channel(conn, payload, ts, recv_lc):
    clock_update(recv_lc)
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    if not channel or " " in channel:
        return pack("CHANNEL_ERROR", {"reason": "invalid_channel_name"})
    with _db_lock:
        if channel_exists(conn, channel):
            return pack("CHANNEL_EXISTS", {"channel": channel})
        lc = clock_get()
        conn.execute("INSERT INTO channels (name, created_by, timestamp, logical_clock) VALUES (?,?,?,?)",
                     (channel, username, ts, lc))
        conn.commit()
    log(f"CHANNEL_CREATED: '{channel}' | lc={lc}")
    return pack("CHANNEL_CREATED", {"channel": channel})


def handle_publish(conn, pub_socket, payload, ts, recv_lc):
    clock_update(recv_lc)
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    message  = payload.get("message", "").strip()

    with _db_lock:
        if not channel or not channel_exists(conn, channel):
            return pack("PUBLISH_ERROR", {"reason": "channel_not_found"})
    if not message:
        return pack("PUBLISH_ERROR", {"reason": "empty_message"})

    lc  = clock_tick()
    now = physical_now_ms()
    pub_payload = msgpack.packb({
        "channel": channel, "username": username, "message": message,
        "timestamp": now, "logical_clock": lc,
    }, use_bin_type=True)
    pub_socket.send_multipart([channel.encode(), pub_payload])

    with _db_lock:
        conn.execute("INSERT INTO publications (channel, username, message, timestamp, logical_clock)"
                     " VALUES (?,?,?,?,?)", (channel, username, message, now, lc))
        conn.commit()

    log(f"PUBLISH_OK: canal='{channel}' | user='{username}' | lc={lc}")
    return pack("PUBLISH_OK", {"channel": channel, "timestamp": ts})


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def main():
    global _zmq_context_global
    _msg_count = 0

    conn    = init_db(DB_PATH)
    context = zmq.Context()
    _zmq_context_global = context

    register_with_reference(context)

    # Socket PUB para publicar nos canais e no tópico 'servers'
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{PROXY_PORT}")

    # Thread SUB — escuta anúncios de coordenador
    threading.Thread(target=coordinator_subscriber, args=(context,), daemon=True).start()

    # Thread REP servidor↔servidor
    threading.Thread(target=s2s_server, args=(context, pub_socket), daemon=True).start()

    time.sleep(1)

    # Eleição inicial
    log("Iniciando eleição inicial...")
    threading.Thread(target=start_election, args=(pub_socket,), daemon=True).start()

    time.sleep(1)

    # Socket REP para clientes (via broker)
    rep_socket = context.socket(zmq.REP)
    rep_socket.connect(f"tcp://{BROKER_HOST}:{BROKER_PORT}")
    log(f"Servidor pronto | rank={get_rank()} | coordenador={get_coordinator()} | lc={clock_get()}")

    while True:
        try:
            raw      = rep_socket.recv()
            msg      = unpack(raw)
            msg_type = msg.get("type", "")
            payload  = msg.get("payload", {})
            ts       = msg.get("timestamp", physical_now_ms())
            recv_lc  = msg.get("logical_clock", 0)

            log(f"RECEBIDO | type={msg_type} | lc_msg={recv_lc} | lc_local={clock_get()}")

            if msg_type == "LOGIN":
                response = handle_login(conn, payload, ts, recv_lc)
            elif msg_type == "LIST_CHANNELS":
                response = handle_list_channels(conn, payload, recv_lc)
            elif msg_type == "CREATE_CHANNEL":
                response = handle_create_channel(conn, payload, ts, recv_lc)
            elif msg_type == "PUBLISH":
                response = handle_publish(conn, pub_socket, payload, ts, recv_lc)
            else:
                clock_update(recv_lc)
                response = pack("ERROR", {"reason": f"unknown_type: {msg_type}"})

            rep_socket.send(response)

            _msg_count += 1

            # Heartbeat na referência a cada 10 mensagens
            if _msg_count % HEARTBEAT_EVERY == 0:
                threading.Thread(target=do_heartbeat, daemon=True).start()

            # Sincronização do relógio a cada 15 mensagens
            if _msg_count % SYNC_EVERY == 0:
                threading.Thread(target=sync_clock_with_coordinator,
                                 args=(pub_socket,), daemon=True).start()

        except Exception as e:
            log(f"ERRO: {e}")
            try:
                rep_socket.send(pack("ERROR", {"reason": str(e)}))
            except:
                pass


if __name__ == "__main__":
    main()
