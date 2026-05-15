# Método de Replicação Escolhido

O método de replicação escolhido para o projeto foi a replicação ativa síncrona simplificada entre os servidores.

## Motivação

O broker utiliza balanceamento de carga round-robin entre os servidores.  
Sem replicação, cada servidor armazenaria apenas parte das operações realizadas pelos clientes, causando:

- inconsistência entre servidores
- perda de mensagens em caso de falha
- histórico incompleto de canais e publicações

Para evitar esse problema, foi implementado um mecanismo de replicação entre todos os servidores.

---

# Funcionamento da Replicação

Sempre que um servidor recebe uma operação crítica de um cliente, ele:

1. Processa a operação localmente
2. Persiste os dados no banco SQLite local
3. Replica a operação para os demais servidores
4. Aguarda confirmação (`REPLICATION_OK`)

As operações replicadas são:

- LOGIN
- CREATE_CHANNEL
- PUBLISH

---

# Comunicação Entre Servidores

A replicação utiliza sockets REQ/REP dedicados para comunicação servidor↔servidor.

Foi criada uma nova mensagem do tipo:

```json
{
  "type": "REPLICATION",
  "operation": "PUBLISH",
  "data": {
    "channel": "canal-x",
    "username": "usuario",
    "message": "mensagem"
  }
}