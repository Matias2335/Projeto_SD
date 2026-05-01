# BBS - Bulletin Board System (Parte 4)

Sistema de troca de mensagens inspirado nos antigos BBS/IRC, desenvolvido para a disciplina de Sistemas Distribuídos.

---

## O que esse projeto faz?

Simula um sistema de mensagens onde **bots** (clientes automáticos) se conectam a servidores, criam canais e publicam mensagens continuamente — sem nenhuma interação humana.

Ao iniciar, cada bot executa automaticamente:

1. **Login** no servidor
2. **Verifica canais** — se houver menos de 5, cria um novo
3. **Inscreve** em pelo menos 3 canais aleatórios para receber mensagens
4. **Loop infinito** — escolhe um canal aleatório e envia 10 mensagens com intervalo de 1 segundo

---

## Como executar

Você só precisa ter o **Docker** instalado. Depois, rode:

```bash
docker compose up --build
```

Para parar, pressione `Ctrl + C`.

> Se aparecer erro de container com nome em conflito, rode antes:
> ```bash
> docker compose down -v
> ```

---

## O que aparece no terminal?

Eleição e definição do coordenador:
```
[python-server-1 21:00:01] ELEIÇÃO iniciada | meu rank=1
[python-server-1 21:00:01] ELEIÇÃO → ninguém respondeu, sou o coordenador!
[python-server-1 21:00:01] ★ Coordenador definido: 'python-server-1'
[python-server-1 21:00:01] ANUNCIO coordenador='python-server-1' publicado no tópico 'servers'
[python-server-2 21:00:01] SUB [servers] novo coordenador='python-server-1' | lc=3
```

Sincronização de relógio entre servidores:
```
[python-server-2 21:00:15] SYNC OK | coord='python-server-1' | coord_clock=1714000815123 | offset=+1ms | lc=22
```

Mensagens trocadas via REQ/REP (com relógio lógico):
```
[bot-python-1 21:00:05] ENVIANDO | type=PUBLISH | lc=5 | payload={...}
[bot-python-1 21:00:05] RECEBIDO | type=PUBLISH_OK | lc_msg=8 | lc_local=9
```

Mensagens recebidas via PUB/SUB:
```
[bot-python-1 21:00:05] [SUB] canal='geral' | de='bot-js-1' | msg='Olá!' | lc=7 | ts_envio=... | ts_recebimento=...
```

---

## Arquitetura

```
                              ┌───────────────────┐
  Servidor ── GET_RANK ──────►│                   │
  Servidor ── HEARTBEAT ─────►│    Referência     │
  Servidor ── LIST_SERVERS ──►│    (porta 5560)   │
                              └───────────────────┘

                    S2S (porta 6000 de cada servidor)
         ┌──────────────────────────────────────────┐
         │  ELECTION / ELECTION_OK / GET_CLOCK / CLOCK│
         └──────────────────────────────────────────┘
  Servidor A ◄──────────────────────────────────► Servidor B

┌─────────────┐   REQ (5555)  ┌────────┐  REP (5556)  ┌──────────────────┐
│    bots     │──────────────►│ broker │─────────────►│   servidores     │
│  (clientes) │               └────────┘               │ (Python + JS)    │
└─────────────┘                                        └────────┬─────────┘
       ▲                                                        │ PUB (5557)
       │ SUB (5558)                                             ▼
       │                                              ┌─────────────────┐
       └─────────────────────────────────────────────│  proxy PUB/SUB  │◄── tópico 'servers'
                                                      └─────────────────┘    (anúncio de eleição)
```

| Componente | Qtd | Função |
|---|---|---|
| **Referência** | 1 | Atribui ranks, mantém lista de servidores ativos via heartbeat |
| **Broker** | 1 | Distribui requisições REQ/REP entre os servidores (round-robin) |
| **Proxy PUB/SUB** | 1 | Repassa publicações e anúncios de eleição (`servers`) |
| **Servidores** | 4 (2 Python + 2 JS) | Processam requisições, realizam eleição, sincronizam relógio |
| **Bots (clientes)** | 4 (2 Python + 2 JS) | Publicam mensagens e recebem publicações |

---

## Estrutura de arquivos

```
Projeto_SD-parte4/
├── docker-compose.yaml     ← orquestra todos os containers
├── README.md               ← este arquivo
├── PROTOCOL.md             ← documentação técnica do protocolo
├── reference/              ← serviço de referência  ← MODIFICADO (sem clock no heartbeat)
│   ├── Dockerfile
│   └── reference.py
├── broker/                 ← broker REQ/REP (porta 5555/5556)  ← sem alteração
│   ├── Dockerfile
│   └── broker.py
├── proxy-pubsub/           ← proxy PUB/SUB (porta 5557/5558)  ← sem alteração
│   ├── Dockerfile
│   └── proxy.py
├── server-python/          ← servidor Python  ← MODIFICADO (eleição + Berkeley)
│   ├── Dockerfile
│   └── server.py
├── client-python/          ← bot Python  ← sem alteração
│   ├── Dockerfile
│   └── client.py
├── server-js/              ← servidor JavaScript  ← MODIFICADO (eleição + Berkeley)
│   ├── Dockerfile
│   ├── package.json
│   └── server.js
└── client-js/              ← bot JavaScript  ← sem alteração
    ├── Dockerfile
    ├── package.json
    └── client.js
```

---

## Novidades da Parte 4

### Eleição — algoritmo de Bully adaptado

Cada servidor recebe um rank único do serviço de referência. O **menor rank tem maior prioridade** e se torna coordenador.

O processo de eleição ocorre em dois momentos: na inicialização de cada servidor, e quando o coordenador atual não responde a uma requisição de sincronização de relógio.

**Fluxo da eleição:**

1. O servidor que inicia a eleição consulta `LIST_SERVERS` na referência para obter todos os servidores ativos
2. Contacta apenas os servidores com rank menor (maior prioridade) enviando `ELECTION` via socket S2S (porta 6000)
3. Se algum responder `ELECTION_OK`, esse servidor assumirá a eleição — o iniciador aguarda
4. Se nenhum responder dentro de 2 segundos, o servidor se proclama coordenador
5. O novo coordenador publica no tópico `servers` do proxy PUB/SUB: `{ "coordinator": "nome" }`
6. Todos os servidores estão inscritos no tópico `servers` e atualizam sua variável local `coordinator`

### Sincronização de relógio — algoritmo de Berkeley simplificado

A cada **15 mensagens** de clientes recebidas, o servidor sincroniza seu relógio com o coordenador:

1. Envia `GET_CLOCK` ao coordenador via socket S2S com timeout de 2 segundos
2. O coordenador responde com `CLOCK { "clock": timestamp_atual }`
3. O servidor calcula o offset aplicando RTT/2:

```
offset = clock_coordenador + RTT/2 - timestamp_recebimento_local
```

4. O offset é somado a todos os timestamps produzidos pelo servidor dali em diante
5. Se o coordenador não responder, uma nova eleição é iniciada automaticamente

### Comunicação S2S (servidor ↔ servidor)

Cada servidor expõe um socket REP na **porta 6000** exclusivamente para outros servidores. Os endereços de cada peer são injetados via variável de ambiente `S2S_PEERS` no `docker-compose.yaml`:

```
S2S_PEERS=python-server-2=bbs-server-python-2:6000,js-server-1=bbs-server-js-1:6000,...
```

Mensagens suportadas: `ELECTION` / `ELECTION_OK` / `GET_CLOCK` / `CLOCK`

### Mudança no serviço de referência

O heartbeat não retorna mais o campo `clock`. A resposta `HEARTBEAT_OK` agora tem payload vazio `{}`. A sincronização de relógio físico passou a ser responsabilidade dos próprios servidores via eleição e Berkeley.

---

## Escolhas técnicas

### Linguagens
- **Python 3.13** — servidor, cliente e serviço de referência
- **JavaScript com Node.js 20** — servidor e cliente

### Serialização: MessagePack
Todas as mensagens são serializadas em binário com MessagePack. Toda mensagem carrega `type`, `timestamp`, `logical_clock` e `payload`.

### Padrão de mensagens ZeroMQ

| Canal | Padrão | Uso |
|---|---|---|
| Clientes → Servidores | REQ/REP via ROUTER/DEALER (broker) | Login, canais, publicação |
| Servidores → Referência | REQ/REP direto | Rank, heartbeat, lista |
| Servidor ↔ Servidor | REQ/REP direto (porta 6000) | Eleição e sincronização de relógio |
| Servidores → Todos | PUB/SUB via XSUB/XPUB (proxy) | Mensagens de canal + anúncio de eleição |

### Persistência: SQLite compartilhado

Todos os servidores acessam o mesmo arquivo `/data/bbs.db` via volume Docker compartilhado. As tabelas incluem o campo `logical_clock` para reconstrução da ordem causal dos eventos.

| Tabela | O que guarda |
|---|---|
| `logins` | username + timestamp + logical_clock |
| `channels` | nome + criador + timestamp + logical_clock |
| `publications` | canal + username + mensagem + timestamp + logical_clock |

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
| Falha de comunicação com cliente | Bot recria o socket e tenta novamente em 2s |
| Coordenador não responde (timeout 2s) | Servidor inicia nova eleição automaticamente |
| Servidor sem heartbeat por 60s | Removido da lista pelo watchdog da referência |
| Container com nome em conflito | `docker compose down -v` e subir novamente |
