# stellar-pipelines

This repository provides a collection of **SQL pipelines** for generating [**Apache Iceberg tables**](https://iceberg.apache.org/) from the [**Stellar blockchain’s history**](https://stellar.org/) using **[AGT](https://github.com/agnosticeng/agt)**.
These pipelines leverages ClickHouse and [`ch-stellar`](https://github.com/agnosticeng/ch-stellar), a library of ClickHouse UDFs that simplifies extraction and processing of Stellar data.

## Pipelines

|path|description|
|-|-|
| [ledgers](./ledgers/) | Extracts and normalizes ledger metadata (sequence, close time, base fee, etc.) |
| [transactions](./transactions/) |	Parses and structures transaction-level data including source, fees, and memo |
| [operations](./operations/) |	Extracts all operation types with relevant parameters and participants |
| [accounts](./accounts/) |	Builds current and historical account state tables |
| [contracts_data](./contract_data/) | Processes smart contract data and state changes (Soroban) |
| [changes](./changes/) | Captures ledger changes |
| [events](./events/) |	Normalizes emitted Soroban contract events |
| [liquidity_pools](./liquidity_pools/) | Tracks pool creation, reserves, and share supply |
| [trades](./trades/) |	Extracts trade data from offers and path payments |
| [trustlines](./trustlines/) | Maps trustline relationships and balance changes |

## Running with AGT

Here we show how to run the `ledgers` pipeline, sorting the produced table by the `sequence` column.

```sh
agt run \
    --var="ICEBERG_URL=MY_S3_LOCATION" \
    --var="GALEXIE_URL=MY_GALEXIE_URL" \
    --var="RPC_URL=https://stellar-soroban-testnet-public.nodies.app" \
    ../stellar-pipelines/ledgers/pipeline.yaml
```
