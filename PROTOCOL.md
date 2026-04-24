# Protocolo de Mensagens BBS — Parte 3

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

1. **Antes de enviar**: `lc = lc + 1` — o valor é incluído na mensagem
2. **Ao receber**: `lc = max(lc_local, lc_recebido) + 1`

## Tipos de mensagem — REQ/REP (cliente ↔ servidor via broker)

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

## Tipos de mensagem — REQ/REP (servidor ↔ serviço de referência)

### Servidor → Referência

| type          | payload                | Descrição                                         |
|---------------|------------------------|---------------------------------------------------|
| GET_RANK      | { "name": string }     | Registro e obtenção de rank                       |
| LIST_SERVERS  | {}                     | Lista todos os servidores disponíveis             |
| HEARTBEAT     | { "name": string }     | Keepalive + sincronização do relógio físico       |

### Referência → Servidor

| type           | payload                                              |
|----------------|------------------------------------------------------|
| RANK           | { "rank": int }                                      |
| SERVER_LIST    | { "servers": [ {"name": string, "rank": int}, ... ]} |
| HEARTBEAT_OK   | { "clock": int }  ← timestamp atual da referência   |
| ERROR          | { "reason": string }                                 |

## Mensagens PUB/SUB (servidor → proxy → clientes inscritos)

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

## Sincronização do relógio físico (algoritmo de Cristian)

```
t_send  → servidor envia HEARTBEAT
t_recv  ← servidor recebe HEARTBEAT_OK com clock=T_ref
offset  = T_ref + (t_recv - t_send)/2 - t_recv
```

O servidor aplica `offset` a todos os timestamps produzidos dali em diante.

## Heartbeat

- Enviado pelo servidor a cada **10 mensagens de clientes** recebidas
- Se um servidor não enviar heartbeat por **60 segundos**, é removido da lista de servidores disponíveis

## Erros definidos

| reason               | Situação                                    |
|----------------------|---------------------------------------------|
| invalid_username     | Username vazio                              |
| already_logged_in    | Username já está logado                     |
| invalid_channel_name | Nome de canal vazio ou com espaços          |
| channel_not_found    | Canal não existe ao tentar publicar         |
| empty_message        | Mensagem vazia ao tentar publicar           |
| missing_name         | Campo "name" ausente no pedido ao reference |
