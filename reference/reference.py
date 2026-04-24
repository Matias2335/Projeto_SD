"""
Serviço de Referência — Parte 3
Responsabilidades:
  1. Atribuir rank aos servidores que se registram
  2. Manter lista de servidores disponíveis
  3. Responder pedidos de lista de servidores
  4. Receber heartbeat e remover servidores inativos
  5. Fornecer horário atual para sincronização do relógio físico
"""

import os
import time
import threading
import zmq
import msgpack

REQ_PORT     = int(os.environ.get("REF_REQ_PORT",  "5560"))
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "60"))  # segundos


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[REFERENCE {ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Estado compartilhado (protegido por lock)
# ---------------------------------------------------------------------------
lock         = threading.Lock()
# { nome: { "rank": int, "last_seen": float } }
servers: dict = {}
next_rank     = 1


def register_server(name: str) -> int:
    """Registra servidor (sem duplicata) e retorna seu rank."""
    global next_rank
    with lock:
        if name in servers:
            return servers[name]["rank"]
        rank = next_rank
        next_rank += 1
        servers[name] = {"rank": rank, "last_seen": time.time()}
        log(f"Servidor registrado: '{name}' → rank {rank}")
        return rank


def heartbeat(name: str) -> bool:
    """Atualiza last_seen de um servidor. Retorna False se não estava registrado."""
    with lock:
        if name not in servers:
            return False
        servers[name]["last_seen"] = time.time()
        return True


def get_server_list() -> list:
    """Retorna lista de servidores ativos [ {name, rank}, ... ]."""
    with lock:
        return [{"name": n, "rank": d["rank"]} for n, d in servers.items()]


# ---------------------------------------------------------------------------
# Thread de watchdog — remove servidores sem heartbeat
# ---------------------------------------------------------------------------
def watchdog():
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            dead = [n for n, d in servers.items()
                    if now - d["last_seen"] > HEARTBEAT_TIMEOUT]
            for name in dead:
                log(f"Servidor removido por inatividade: '{name}'")
                del servers[name]


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
def handle(msg: dict) -> dict:
    msg_type = msg.get("type", "")
    payload  = msg.get("payload", {})

    # ── REQ: rank  ──────────────────────────────────────────────────────────
    if msg_type == "GET_RANK":
        name = payload.get("name", "").strip()
        if not name:
            return {"type": "ERROR", "payload": {"reason": "missing_name"}}
        rank = register_server(name)
        log(f"GET_RANK ← '{name}' → rank={rank}")
        return {"type": "RANK", "payload": {"rank": rank}}

    # ── REQ: list  ──────────────────────────────────────────────────────────
    elif msg_type == "LIST_SERVERS":
        srv_list = get_server_list()
        log(f"LIST_SERVERS → {len(srv_list)} servidor(es)")
        return {"type": "SERVER_LIST", "payload": {"servers": srv_list}}

    # ── REQ: heartbeat  ─────────────────────────────────────────────────────
    elif msg_type == "HEARTBEAT":
        name = payload.get("name", "").strip()
        if not name:
            return {"type": "ERROR", "payload": {"reason": "missing_name"}}
        # Se o servidor ainda não se registrou, registra agora
        register_server(name)
        heartbeat(name)
        now_ms = int(time.time() * 1000)
        log(f"HEARTBEAT ← '{name}' | clock={now_ms}")
        # Retorna OK + horário atual para sincronização do relógio físico
        return {"type": "HEARTBEAT_OK", "payload": {"clock": now_ms}}

    else:
        log(f"Tipo desconhecido: '{msg_type}'")
        return {"type": "ERROR", "payload": {"reason": f"unknown_type: {msg_type}"}}


# ---------------------------------------------------------------------------
# Loop principal REP
# ---------------------------------------------------------------------------
def main():
    threading.Thread(target=watchdog, daemon=True).start()

    context = zmq.Context()
    rep = context.socket(zmq.REP)
    rep.bind(f"tcp://*:{REQ_PORT}")
    log(f"Serviço de referência iniciado na porta {REQ_PORT}")
    log(f"Timeout de heartbeat: {HEARTBEAT_TIMEOUT}s")

    while True:
        try:
            raw  = rep.recv()
            msg  = msgpack.unpackb(raw, raw=False)
            resp = handle(msg)
            resp["timestamp"] = int(time.time() * 1000)
            rep.send(msgpack.packb(resp, use_bin_type=True))
        except Exception as e:
            log(f"ERRO: {e}")
            err = msgpack.packb(
                {"type": "ERROR", "payload": {"reason": str(e)},
                 "timestamp": int(time.time() * 1000)},
                use_bin_type=True
            )
            rep.send(err)


if __name__ == "__main__":
    main()
