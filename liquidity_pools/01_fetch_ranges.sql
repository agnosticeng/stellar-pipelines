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
                assumenNotNull(
                    coalesce(
                        ledger_close_meta.v0.ledger_header.header.ledger_seq::Nullable(UInt32),
                        ledger_close_meta.v1.ledger_header.header.ledger_seq::Nullable(UInt32),
                        ledger_close_meta.v2.ledger_header.header.ledger_seq::Nullable(UInt32)
                    )
                ) as ledger_sequence,

                coalesce(
                    ledger_close_meta.v0.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
                    ledger_close_meta.v1.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
                    ledger_close_meta.v2.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC'))
                ) as ledger_closed_at,

                coalesce(
                    ledger_close_meta.v0.ledger_header.hash::Nullable(String),
                    ledger_close_meta.v1.ledger_header.hash::Nullable(String),
                    ledger_close_meta.v2.ledger_header.hash::Nullable(String)
                ) as ledger_hash,

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
                ) as tx_metas

            from galexie
        ),

        tx_envelopes as (
            select
                * except (tx_envelopes),
                tx_envelope
            from ledgers 
            array join 
                tx_envelopes as tx_envelope
            ),

        tx_metas as (
            select 
                tx_meta,
                tx_order
            from ledgers
            array join 
                tx_metas as tx_meta,
                arrayEnumerate(tx_metas) as tx_order
        ),

        txs as (
            select 
                ledger_sequence,
                ledger_closed_at,
                ledger_hash,
                stellar_hash_transaction(tx_envelope::String, 'Public Global Stellar Network ; September 2015') as hash,
                stellar_id(ledger_sequence::Int32, tx_order::Int32, 0::Int32) as id,
                splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1])[1] as result_code,
                
                firstNonDefault(
                    tx_envelope.tx.tx.source_account::String,
                    tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.source_account::String
                ) as source_account,

                tx_order,

                arrayConcat(
                    tx_envelope.tx.tx.operations[],
                    tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.operations[]
                ) as ops,

                arrayConcat(
                    tx_meta.tx_apply_processing[],
                    tx_meta.tx_apply_processing.v1.operations[],
                    tx_meta.tx_apply_processing.v2.operations[],
                    tx_meta.tx_apply_processing.v3.operations[],
                    tx_meta.tx_apply_processing.v4.operations[]
                ) as ops_meta,

                tx_meta.tx_apply_processing.v4.events[] as events
            from tx_envelopes
            left join tx_metas
            on hash = tx_meta.result.transaction_hash::String
        ),

        ops as (
            select 
                ledger_sequence,
                ledger_closed_at,
                ledger_hash,
                hash as transaction_hash,
                id as transaction_id,
                result_code as transaction_result_code,
                source_account as transaction_source_account,
                stellar_id(ledger_sequence::Int32, tx_order::Int32, op_order::Int32) as id,
                splitByChar('.', JSONAllPaths(op.^body)[1])[1] as type,
                op,
                op_meta
            from txs
            array join 
                ops as op,
                ops_meta as op_meta,
                arrayEnumerate(ops) as op_order
        ),

        tx_events as (
            select 
                ledger_sequence,
                ledger_closed_at,
                ledger_hash,
                hash as transaction_hash,
                result_code as transaction_result_code,
                source_account as transaction_source_account,
                null as operation_id,
                event.^event::JSON as event
            from txs
            array join events as event
        ),

        ops_events as (
            select
                ledger_sequence,
                ledger_closed_at,
                ledger_hash,
                transaction_hash,
                transaction_result_code,
                transaction_source_account,
                id as operation_id,
                event::JSON as event
            from ops
            array join op_meta.events[] as event
        ),

        all_events as (
            select * from tx_events
            union all
            select * from ops_events
        )

    select 
        * except event,  
        event.contract_id::String as contract_id,
        event.type_::String as type,
        event.body.v0.topics[] as topics,
        event.^body.v0.data as data
    from all_events
)

{{end}}