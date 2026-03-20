# BBS - Bulletin Board System (Parte 1)

Sistema de troca de mensagens inspirado nos antigos BBS/IRC, desenvolvido para a disciplina de Sistemas Distribuídos.

## Integrante

Projeto individual — 2 linguagens: **Python** e **JavaScript (Node.js)**.

## Arquitetura

```
[bot-python-1] ──┐
[bot-python-2] ──┤                    ┌── [server-python-1]
[bot-js-1]     ──┼──► BROKER (ZMQ) ──┤── [server-python-2]
[bot-js-2]     ──┘    ROUTER/DEALER  ├── [server-js-1]
                                      └── [server-js-2]
```

- **Clientes (bots)** conectam no broker via ZeroMQ REQ na porta `5555`
- **Broker** usa o padrão `ROUTER/DEALER` para distribuir as requisições entre os servidores disponíveis (balanceamento round-robin automático)
- **Servidores** conectam no broker via ZeroMQ REP na porta `5556`

## Escolhas técnicas

### Linguagens
- **Python 3.12** — servidor e cliente
- **JavaScript (Node.js 20)** — servidor e cliente

### Serialização: MessagePack
Todas as mensagens trocadas são serializadas em binário usando **MessagePack**.  
Motivo da escolha: formato binário compacto, suporte nativo em Python (`msgpack`) e JavaScript (`@msgpack/msgpack`), e fácil de usar sem geração de código.

Estrutura base de toda mensagem:
```
{
  "type":      string,   // tipo da operação
  "timestamp": int,      // unix timestamp em milissegundos
  "payload":   map       // dados específicos do tipo
}
```

### Persistência: SQLite
Cada servidor mantém seu próprio banco de dados SQLite (arquivo `.db` isolado por volume Docker).  
Motivo da escolha: zero configuração, suporte nativo em Python e JavaScript (via `better-sqlite3`), e os dados ficam em um único arquivo portátil.

Tabelas:
- `logins(id, username, timestamp)` — registra cada login com horário
- `channels(name, created_by, timestamp)` — canais criados e por quem

### Padrão de mensagens ZeroMQ
`ROUTER/DEALER` no broker + `REQ` nos clientes + `REP` nos servidores.  
Esse padrão permite que múltiplos servidores se conectem ao broker e recebam requisições de forma balanceada (round-robin), sem precisar de endereçamento explícito.

## Como executar

```bash
docker compose up --build
```

Todos os containers sobem automaticamente. Os bots realizam:
1. Login no servidor
2. Listagem dos canais disponíveis
3. Criação de um canal
4. Nova listagem para confirmar

Não há interação manual necessária.

## Estrutura de arquivos

```
bbs-project/
├── docker-compose.yaml
├── PROTOCOL.md
├── README.md
├── broker/
│   ├── Dockerfile
│   └── broker.py
├── server-python/
│   ├── Dockerfile
│   └── server.py
├── client-python/
│   ├── Dockerfile
│   └── client.py
├── server-js/
│   ├── Dockerfile
│   ├── package.json
│   └── server.js
└── client-js/
    ├── Dockerfile
    ├── package.json
    └── client.js
```

## Tratamento de erros

| Situação | Comportamento |
|---|---|
| Username vazio | `LOGIN_ERROR` com reason `invalid_username` |
| Login duplicado (mesmo username) | `LOGIN_ERROR` com reason `already_logged_in` |
| Nome de canal com espaço ou vazio | `CHANNEL_ERROR` com reason `invalid_channel_name` |
| Canal já existente | `CHANNEL_EXISTS` (não é erro fatal — cliente usa o canal existente) |
| Falha no login | Cliente recria o socket e tenta novamente após 2 segundos |
