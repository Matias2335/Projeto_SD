# BBS - Bulletin Board System (Parte 1 e 2)

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

Mensagens enviadas pelo bot para o servidor (via REQ/REP):
```
[bot-js-1 10:00:01] ENVIANDO | type=PUBLISH | payload={"channel":"geral","message":"Olá!"} | timestamp=...
[bot-js-1 10:00:01] RECEBIDO | type=PUBLISH_OK | payload={"channel":"geral"} | timestamp=...
```

Mensagens recebidas via PUB/SUB (de qualquer bot inscrito no canal):
```
[bot-python-1 10:00:01] [SUB] MENSAGEM RECEBIDA | canal='geral' | de='bot-js-1' | msg='Olá!' | timestamp_envio=... | timestamp_recebimento=...
```

---

## Arquitetura

O projeto usa dois fluxos de comunicação paralelos:

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
```

| Componente | Qtd | Função |
|---|---|---|
| **Broker** | 1 | Distribui requisições REQ/REP entre os servidores (round-robin) |
| **Proxy PUB/SUB** | 1 | Repassa publicações dos servidores para os bots inscritos |
| **Servidores** | 4 (2 Python + 2 JS) | Processam requisições, publicam mensagens e salvam dados |
| **Bots (clientes)** | 4 (2 Python + 2 JS) | Publicam mensagens e recebem publicações dos outros bots |

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
  "type":      string,   // tipo da operação (LOGIN, PUBLISH, etc.)
  "timestamp": int,      // momento do envio em milissegundos
  "payload":   map       // dados específicos da operação
}
```

### Padrão de mensagens ZeroMQ
- **REQ/REP via broker (ROUTER/DEALER):** clientes enviam requisições (login, criar canal, publicar) e recebem respostas do servidor
- **PUB/SUB via proxy (XSUB/XPUB):** servidores publicam mensagens no proxy, bots inscritos recebem automaticamente. O tópico de cada mensagem é o **nome do canal**

### Persistência: SQLite
Cada servidor salva seus dados em um arquivo `.db` isolado. As tabelas são:

| Tabela | O que guarda |
|---|---|
| `logins` | username + timestamp de cada login |
| `channels` | nome do canal + quem criou + timestamp |
| `publications` | canal + username + mensagem + timestamp de cada publicação |

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
