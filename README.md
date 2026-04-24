# BBS - Bulletin Board System (Parte 3)

Sistema de troca de mensagens inspirado nos antigos BBS/IRC, desenvolvido para a disciplina de Sistemas Distribuídos.

---

## O que esse projeto faz?

Simula um sistema de mensagens onde **bots** (clientes automáticos) se conectam a servidores, criam canais e publicam mensagens continuamente — sem nenhuma interação humana.

Ao iniciar, cada bot executa automaticamente:

1. **Login** no servidor
2. **Verifica canais** — se houver menos de 5, cria um novo
3. **Inscreve** em pelo menos 3 canais aleatórios para receber mensagens
4. **Loop infinito** — escolhe um canal aleatório e envia 10 mensagens com intervalo de 1 segundo, repetindo para sempre

---

## Como executar

Você só precisa ter o **Docker** instalado. Depois, rode:

```bash
docker compose up --build
```

Para parar, pressione `Ctrl + C`.

> Não é necessária nenhuma configuração manual. Tudo sobe e funciona sozinho.

---

## O que aparece no terminal?

Mensagens enviadas pelo bot para o servidor (via REQ/REP), agora com relógio lógico (`lc`):
```
[bot-js-1 10:00:01] ENVIANDO | type=PUBLISH | lc=84 | payload={"username":"bot-js-1","channel":"geral","message":"Olá!"}
[bot-js-1 10:00:01] RECEBIDO | type=PUBLISH_OK | lc_msg=85 | lc_local=86 | payload={"channel":"geral","timestamp":...}
```

Mensagens recebidas via PUB/SUB, com relógio lógico e timestamps de envio/recebimento:
```
[bot-python-1 10:00:01] [SUB] canal='geral' | de='bot-js-1' | msg='Olá!' | lc=85 | ts_envio=... | ts_recebimento=...
```

Heartbeat dos servidores para o serviço de referência, com sincronização de relógio físico:
```
[REFERENCE 10:00:02] HEARTBEAT ← 'python-server-1' | clock=1777050022186
[python-server-1 10:00:02] HEARTBEAT OK | clock_ref=1777050022186 | offset=+0ms
```

---

## Arquitetura

O projeto usa dois fluxos de comunicação paralelos, além de um **serviço de referência** novo na Parte 3:

```
┌─────────────┐     REQ (5555)    ┌────────┐    REP (5556)    ┌──────────────────┐
│  bots       │ ────────────────► │ broker │ ───────────────► │ servidores       │
│  (clientes) │                   └────────┘                   │ (Python + JS)    │
└─────────────┘                                                └────────┬─────────┘
       ▲                                                                │ PUB (5557)
       │ SUB (5558)                                                     ▼
       │                                                       ┌────────────────┐
       └──────────────────────────────────────────────────────│  proxy PUB/SUB │
                                                               └────────────────┘

                                          REQ (5560)    ┌───────────────────────┐
       servidores ──────────────────────────────────────► serviço de referência │
                                                         │ (rank + heartbeat)   │
                                                         └───────────────────────┘
```

| Componente | Qtd | Função |
|---|---|---|
| **Broker** | 1 | Distribui requisições REQ/REP entre os servidores (round-robin) |
| **Proxy PUB/SUB** | 1 | Repassa publicações dos servidores para os bots inscritos |
| **Servidores** | 4 (2 Python + 2 JS) | Processam requisições, publicam mensagens, mantêm relógio de Lamport e enviam heartbeat |
| **Bots (clientes)** | 4 (2 Python + 2 JS) | Publicam mensagens e recebem publicações com relógio de Lamport |
| **Serviço de referência** | 1 | Atribui rank aos servidores, responde lista de servidores ativos e fornece horário para sincronização de relógio físico |

---

## Estrutura de arquivos

```
bbs-project/
├── docker-compose.yaml     ← orquestra todos os containers
├── README.md               ← este arquivo
├── PROTOCOL.md             ← documentação técnica do protocolo
├── broker/                 ← broker REQ/REP (porta 5555/5556)
│   ├── Dockerfile
│   └── broker.py
├── proxy-pubsub/           ← proxy PUB/SUB (porta 5557/5558)
│   ├── Dockerfile
│   └── proxy.py
├── reference/              ← serviço de referência (porta 5560) ← NOVO
│   ├── Dockerfile
│   └── reference.py
├── server-python/          ← servidor Python
│   ├── Dockerfile
│   └── server.py
├── client-python/          ← bot Python
│   ├── Dockerfile
│   └── client.py
├── server-js/              ← servidor JavaScript (Node.js)
│   ├── Dockerfile
│   ├── package.json
│   └── server.js
└── client-js/              ← bot JavaScript (Node.js)
    ├── Dockerfile
    ├── package.json
    └── client.js
```

---

## Escolhas técnicas

### Linguagens
- **Python 3.12** — servidor e cliente
- **JavaScript com Node.js 20** — servidor e cliente

### Serialização: MessagePack
Todas as mensagens (REQ/REP e PUB/SUB) são serializadas em **binário** com MessagePack. Toda mensagem contém:

```
{
  "type":          string,   // tipo da operação (LOGIN, PUBLISH, etc.)
  "timestamp":     int,      // momento do envio em milissegundos
  "logical_clock": int,      // relógio lógico de Lamport do remetente
  "payload":       map       // dados específicos da operação
}
```

### Padrão de mensagens ZeroMQ
- **REQ/REP via broker (ROUTER/DEALER):** clientes enviam requisições (login, criar canal, publicar) e recebem respostas do servidor
- **PUB/SUB via proxy (XSUB/XPUB):** servidores publicam mensagens no proxy, bots inscritos recebem automaticamente. O tópico de cada mensagem é o **nome do canal**
- **REQ/REP com o serviço de referência (porta 5560):** servidores obtêm rank na inicialização e enviam heartbeat periódico

### Persistência: SQLite
Cada servidor salva seus dados em um arquivo `.db` isolado. As tabelas são:

| Tabela | O que guarda |
|---|---|
| `logins` | username + timestamp + logical_clock de cada login |
| `channels` | nome do canal + quem criou + timestamp + logical_clock |
| `publications` | canal + username + mensagem + timestamp + logical_clock de cada publicação |

---

## Novidades da Parte 3

### Relógio lógico de Lamport
Todos os nós (servidores e clientes) mantêm um **relógio lógico de Lamport**. As regras aplicadas são:

- **Antes de enviar:** incrementa o relógio local e inclui o valor no campo `logical_clock` da mensagem
- **Ao receber:** atualiza o relógio local para `max(local, recebido) + 1`

O valor `lc` aparece em todos os logs e é persistido no banco de dados junto a cada evento, permitindo ordenação causal dos eventos mesmo entre servidores distintos.

### Serviço de referência
Um novo container (`bbs-reference`, porta 5560) centraliza três responsabilidades:

| Operação | Quem usa | O que faz |
|---|---|---|
| `GET_RANK` | servidores (na inicialização) | Registra o servidor e retorna um rank único sequencial |
| `HEARTBEAT` | servidores (a cada 10 mensagens processadas) | Atualiza o `last_seen` do servidor e retorna o horário atual do serviço de referência |
| `LIST_SERVERS` | qualquer cliente | Retorna lista dos servidores ativos com nome e rank |

Servidores que não enviam heartbeat por mais de 60 segundos são removidos automaticamente da lista de ativos (watchdog em background).

### Sincronização de relógio físico
A cada heartbeat, o servidor calcula o **offset** entre seu relógio local e o do serviço de referência, compensando a latência de rede (RTT/2). O offset é aplicado a todos os timestamps físicos gerados pelo servidor. Isso é visível no log:

```
[python-server-2 17:00:22] HEARTBEAT OK | clock_ref=1777050022127 | offset=+0ms
```

---

## Tratamento de erros

| Situação | O que acontece |
|---|---|
| Username vazio | `LOGIN_ERROR: invalid_username` |
| Login duplicado | `LOGIN_ERROR: already_logged_in` |
| Nome de canal inválido | `CHANNEL_ERROR: invalid_channel_name` |
| Canal já existente | `CHANNEL_EXISTS` — bot usa o existente |
| Canal não encontrado ao publicar | `PUBLISH_ERROR: channel_not_found` |
| Mensagem vazia ao publicar | `PUBLISH_ERROR: empty_message` |
| Falha de comunicação | Bot recria o socket e tenta novamente em 2s |
| Servidor sem heartbeat por 60s | Removido automaticamente pelo watchdog do serviço de referência |
