{{define "fetch_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with 
        galexie as (
            select 
                ledger_close_meta
            from executable(
                'ch-stellar table-function stellar-galexie',
                ArrowStream,
                'ledger_close_meta JSON',
                (
                    select
                        '{{.GALEXIE_URL}}' as url,
                        {{.RANGE_START}}::UInt32 as start,
                        {{.RANGE_END}}::UInt32 as end
                ),
                settings 
                    stderr_reaction='log', 
                    check_exit_code=true,
                    command_read_timeout=120000
            )
        ),

        ledgers as (
            select 
                assumeNotNull(
                    firstNonDefault(
                        ledger_close_meta.v0.ledger_header.header.ledger_seq::UInt32,
                        ledger_close_meta.v1.ledger_header.header.ledger_seq::UInt32,
                        ledger_close_meta.v2.ledger_header.header.ledger_seq::UInt32
                    )
                ) as sequence,

                firstNonDefault(
                    ledger_close_meta.v0.ledger_header.header.scp_value.close_time::DateTime64(3, 'UTC'),
                    ledger_close_meta.v1.ledger_header.header.scp_value.close_time::DateTime64(3, 'UTC'),
                    ledger_close_meta.v2.ledger_header.header.scp_value.close_time::DateTime64(3, 'UTC')
                ) as closed_at,

                firstNonDefault(
                    ledger_close_meta.v0.ledger_header.hash::String,
                    ledger_close_meta.v1.ledger_header.hash::String,
                    ledger_close_meta.v2.ledger_header.hash::String
                ) as ledger_hash,

                firstNonDefault(
                    ledger_close_meta.v0.ledger_header.header.previous_ledger_hash::String,
                    ledger_close_meta.v1.ledger_header.header.previous_ledger_hash::String,
                    ledger_close_meta.v2.ledger_header.header.previous_ledger_hash::String
                ) as previous_ledger_hash,

                stellar_id(sequence::Int32, 0::Int32, 0::Int32) as id,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.total_coins::Nullable(UInt64),
                    ledger_close_meta.v1.ledger_header.header.total_coins::Nullable(UInt64),
                    ledger_close_meta.v2.ledger_header.header.total_coins::Nullable(UInt64)
                ) as total_coins,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.fee_pool::Nullable(UInt64),
                    ledger_close_meta.v1.ledger_header.header.fee_pool::Nullable(UInt64),
                    ledger_close_meta.v2.ledger_header.header.fee_pool::Nullable(UInt64)
                ) as fee_pool,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.base_fee::Nullable(UInt64),
                    ledger_close_meta.v1.ledger_header.header.base_fee::Nullable(UInt64),
                    ledger_close_meta.v2.ledger_header.header.base_fee::Nullable(UInt64)
                ) as base_fee,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.base_reserve::Nullable(UInt64),
                    ledger_close_meta.v1.ledger_header.header.base_reserve::Nullable(UInt64),
                    ledger_close_meta.v2.ledger_header.header.base_reserve::Nullable(UInt64)
                ) as base_reserve,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.max_tx_set_size::Nullable(UInt32),
                    ledger_close_meta.v1.ledger_header.header.max_tx_set_size::Nullable(UInt32),
                    ledger_close_meta.v2.ledger_header.header.max_tx_set_size::Nullable(UInt32)
                ) as max_tx_set_size,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.ledger_version::Nullable(UInt32),
                    ledger_close_meta.v1.ledger_header.header.ledger_version::Nullable(UInt32),
                    ledger_close_meta.v2.ledger_header.header.ledger_version::Nullable(UInt32)
                ) as ledger_version,

                arrayConcat(
                    ledger_close_meta.v0.tx_set.txs[]::Array(JSON),
                    arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
                    arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON),
                    arrayFlatten(ledger_close_meta.v2.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
                    arrayFlatten(ledger_close_meta.v2.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON)
                ) as tx_envelopes,

                arrayConcat(
                    ledger_close_meta.v0.tx_processing[],
                    ledger_close_meta.v1.tx_processing[],
                    ledger_close_meta.v2.tx_processing[]
                ) as tx_results,

                arrayFilter(
                    res -> splitByChar('.', JSONAllPaths(res.^result.result.result)[1])[1] in ('tx_success', 'tx_fee_bump_inner_success'),
                    tx_results
                ) as successfull_transactions,

                length(tx_results) as transaction_count,
                length(successfull_transactions) as successful_transaction_count,
                (transaction_count - successful_transaction_count) as failed_transaction_count,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.scp_value.ext.signed.node_id::Nullable(String),
                    ledger_close_meta.v1.ledger_header.header.scp_value.ext.signed.node_id::Nullable(String)
                ) as node_id,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.scp_value.ext.signed.signature::Nullable(String),
                    ledger_close_meta.v1.ledger_header.header.scp_value.ext.signed.signature::Nullable(String)
                ) as signature,

                ledger_close_meta.v1.total_byte_size_of_live_soroban_state::Nullable(UInt64) as total_byte_size_of_live_soroban_state,
                ledger_close_meta.v1.ext.v1.soroban_fee_write1_kb::Nullable(UInt64) as soroban_fee_write1_kb,

                arraySum(
                    res -> 
                        length(res.tx_apply_processing[]) +
                        length(res.tx_apply_processing.v1.operations[]) +
                        length(res.tx_apply_processing.v2.operations[]) +
                        length(res.tx_apply_processing.v3.operations[]) +
                        length(res.tx_apply_processing.v4.operations[]),
                    successfull_transactions
                ) as operation_count,

                arraySum(
                    tx -> length(tx.tx.tx.operations[]) + length(tx.tx_fee_bump.tx.inner_tx.tx.tx.operations[]),
                    tx_envelopes
                ) as tx_set_operation_count

            from galexie
        )

    select 
        * except (tx_set, tx_results, successfull_transactions)
    from ledgers
)

{{end}}