# Protocolo de Mensagens BBS — Parte 4

## Serialização
Todas as mensagens são serializadas com **MessagePack**.

## Estrutura base de toda mensagem

```
{
  "type":          string,   // tipo da mensagem
  "timestamp":     int,      // unix timestamp em milissegundos (relógio físico ajustado)
  "logical_clock": int,      // relógio lógico de Lamport do remetente
  "payload":       map       // dados específicos do tipo
}
```

## Relógio lógico de Lamport

Implementado em clientes e servidores:

1. **Antes de enviar**: `lc = lc + 1` — incluído no campo `logical_clock`
2. **Ao receber**: `lc = max(lc_local, lc_recebido) + 1`

---

## REQ/REP — Cliente ↔ Servidor (via broker)

### Cliente → Servidor

| type           | payload                                                      |
|----------------|--------------------------------------------------------------|
| LOGIN          | { "username": string }                                       |
| LIST_CHANNELS  | { "username": string }                                       |
| CREATE_CHANNEL | { "username": string, "channel": string }                    |
| PUBLISH        | { "username": string, "channel": string, "message": string } |

### Servidor → Cliente

| type            | payload                                          |
|-----------------|--------------------------------------------------|
| LOGIN_OK        | { "username": string }                           |
| LOGIN_ERROR     | { "username": string, "reason": string }         |
| CHANNEL_LIST    | { "channels": [string, ...] }                    |
| CHANNEL_CREATED | { "channel": string }                            |
| CHANNEL_EXISTS  | { "channel": string }                            |
| CHANNEL_ERROR   | { "reason": string }                             |
| PUBLISH_OK      | { "channel": string, "timestamp": int }          |
| PUBLISH_ERROR   | { "reason": string }                             |

---

## REQ/REP — Servidor ↔ Serviço de Referência

### Servidor → Referência

| type         | payload                | Descrição                                          |
|--------------|------------------------|----------------------------------------------------|
| GET_RANK     | { "name": string }     | Registro e obtenção de rank                        |
| LIST_SERVERS | {}                     | Lista todos os servidores disponíveis              |
| HEARTBEAT    | { "name": string }     | Keepalive para manter presença na lista            |

### Referência → Servidor

| type          | payload                                               |
|---------------|-------------------------------------------------------|
| RANK          | { "rank": int }                                       |
| SERVER_LIST   | { "servers": [ {"name": string, "rank": int}, ... ] } |
| HEARTBEAT_OK  | {}  ← sem campo clock (mudança da parte 4)            |
| ERROR         | { "reason": string }                                  |

> **Mudança da parte 4:** o campo `clock` foi removido da resposta `HEARTBEAT_OK`.
> A sincronização de relógio agora é feita diretamente entre servidores via eleição e algoritmo de Berkeley.

---

## REQ/REP — Servidor ↔ Servidor (S2S, porta 6000)

Cada servidor expõe uma porta REP dedicada exclusivamente para comunicação com outros servidores.
Os endereços são configurados via variável de ambiente `S2S_PEERS`.

### Eleição

```
Servidor A → Servidor B:  ELECTION  { "candidate": string }
Servidor B → Servidor A:  ELECTION_OK  {}
```

O servidor A contacta apenas servidores com rank menor (maior prioridade).
Se nenhum responder, A se proclama coordenador.
Ao receber `ELECTION`, o servidor B responde `ELECTION_OK` e inicia sua própria eleição.

### Sincronização de relógio (Berkeley simplificado)

```
Servidor → Coordenador:  GET_CLOCK  { "requester": string }
Coordenador → Servidor:  CLOCK      { "clock": int }
```

O servidor calcula o offset usando o algoritmo de Cristian sobre a resposta:
```
offset = clock_coordenador + RTT/2 - timestamp_recebimento
```

---

## PUB/SUB — Servidor → Proxy → Subscribers

### Publicação em canal de usuário

Frame 1 (tópico): nome do canal em bytes
Frame 2 (corpo): MessagePack com:

```
{
  "channel":       string,
  "username":      string,
  "message":       string,
  "timestamp":     int,
  "logical_clock": int
}
```

### Anúncio de coordenador eleito  ← NOVO na parte 4

Frame 1 (tópico): `"servers"` (literal)
Frame 2 (corpo): MessagePack com:

```
{
  "coordinator":   string,   // SERVER_ID do novo coordenador
  "timestamp":     int,
  "logical_clock": int
}
```

Todos os servidores estão inscritos no tópico `servers` e atualizam sua variável
local `coordinator` ao receber este anúncio.

---

## Algoritmo de Eleição — Bully adaptado

- Cada servidor tem um **rank** atribuído pelo serviço de referência (inteiro sequencial)
- **Menor rank = maior prioridade** (rank 1 é o coordenador preferencial)
- Ao detectar falha do coordenador (timeout em `GET_CLOCK`), ou na inicialização,
  o servidor inicia eleição:
  1. Contacta todos os servidores com rank **menor** enviando `ELECTION`
  2. Se algum responder `ELECTION_OK`, aguarda — esse servidor assumirá
  3. Se ninguém responder, proclama-se coordenador e publica em `servers`
- O servidor que recebe `ELECTION` responde `ELECTION_OK` e inicia sua própria eleição

---

## Sincronização de relógio — Berkeley simplificado

- Disparada a cada **15 mensagens** de clientes recebidas
- O servidor envia `GET_CLOCK` ao coordenador atual
- O coordenador responde com `physicalNowMs()` — seu timestamp atual ajustado
- O servidor aplica o offset calculado via RTT/2 a todos os timestamps futuros
- Se o coordenador não responder dentro de **2 segundos**, nova eleição é iniciada

---

## Heartbeat

- Enviado pelo servidor a cada **10 mensagens** de clientes recebidas
- Mantém o servidor na lista da referência
- Não retorna mais horário (mudança da parte 4)
- Servidores sem heartbeat por **60 segundos** são removidos da lista

---

## Erros definidos

| reason               | Situação                                    |
|----------------------|---------------------------------------------|
| invalid_username     | Username vazio                              |
| already_logged_in    | Username já está logado                     |
| invalid_channel_name | Nome de canal vazio ou com espaços          |
| channel_not_found    | Canal não existe ao tentar publicar         |
| empty_message        | Mensagem vazia ao tentar publicar           |
| missing_name         | Campo "name" ausente no pedido ao reference |



