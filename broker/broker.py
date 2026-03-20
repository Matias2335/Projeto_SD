"""
BBS Broker - Roteia mensagens entre clientes e servidores usando ZeroMQ ROUTER/DEALER.

Clientes conectam no FRONTEND (porta 5555) usando REQ.
Servidores conectam no BACKEND (porta 5556) usando REP.
O broker faz o balanceamento de carga round-robin entre os servidores disponíveis.
"""

import zmq
import time


FRONTEND_PORT = 5555  # Clientes conectam aqui
BACKEND_PORT = 5556   # Servidores conectam aqui


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[BROKER {ts}] {msg}", flush=True)


def main():
    context = zmq.Context()

    # ROUTER recebe mensagens dos clientes (REQ)
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(f"tcp://*:{FRONTEND_PORT}")

    # DEALER distribui para os servidores (REP)
    backend = context.socket(zmq.DEALER)
    backend.bind(f"tcp://*:{BACKEND_PORT}")

    log(f"Broker iniciado. Frontend={FRONTEND_PORT}, Backend={BACKEND_PORT}")

    # zmq.proxy faz o roteamento automaticamente (ROUTER <-> DEALER)
    # ROUTER/DEALER preserva os frames de identidade necessários para REQ/REP
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        log("Broker encerrado.")
    finally:
        frontend.close()
        backend.close()
        context.term()


if __name__ == "__main__":
    main()
