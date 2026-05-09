# 01 · CDC em Tempo Real

> A maioria das empresas ainda copia tabela inteira toda hora. CDC captura só o que mudou, em tempo real.

## Problema

Pipelines batch tradicionais sobrecarregam o banco de origem e introduzem latência desnecessária. Change Data Capture resolve isso capturando apenas os eventos de INSERT, UPDATE e DELETE diretamente do write-ahead log (WAL) do banco.

## Solução

Pipeline CDC completo com Debezium lendo o WAL do PostgreSQL e publicando eventos no Kafka. Consumer Python consome e persiste o stream.

```
PostgreSQL (WAL)
    └── Debezium Connector
        └── Kafka Topic: nyc-trips.cdc
            └── Python Consumer
                └── Sink: Parquet / DuckDB
```

## Dataset

**NYC TLC Trip Records**: Dados públicos de corridas de táxi de Nova York.
Fonte: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Stack

| Camada | Tecnologia |
|---|---|
| Fonte | PostgreSQL 16 |
| CDC Connector | Debezium 2.x |
| Message Broker | Apache Kafka |
| Consumer | Python 3.12 + kafka-python |
| Sink | DuckDB / Parquet |
| Infra local | Docker Compose |

## Estrutura

```
cdc-realtime/

```

## Como rodar

```bash
# 1. Criar o ambiente virtual
python -m venv .venv

# 2. Ativar o ambiente
# No Windows:
.venv\Scripts\activate
# No Linux/Mac:
source .venv/bin/activate

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Rode o script python:
./run.py
```

## O que este projeto demonstra

- Configuração de CDC com Debezium + Kafka
- Leitura de eventos de mudança (INSERT / UPDATE / DELETE)
- Schema evolution handling
- Consumer idempotente com retry