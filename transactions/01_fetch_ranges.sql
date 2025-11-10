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
        )

    select
        ledger_sequence,
        ledger_closed_at,
        ledger_hash,

        stellar_hash_transaction(tx_envelope::String, 'Public Global Stellar Network ; September 2015') as hash,
        stellar_id(ledger_sequence::Int32, tx_order::Int32, 0::Int32) as id,

        tx_meta.result.result.result.tx_fee_bump_inner_success.transaction_hash::String as inner_transaction_hash,

        firstNonDefault(
            tx_envelope.tx.tx.source_account::String,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.source_account::String
        ) as account,

        coalesce(
            tx_envelope.tx.tx.seq_num::Nullable(UInt64),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.seq_num::Nullable(UInt64)
        ) as account_sequence,

        (
            length(tx_envelope.tx.tx.operations[]) +
            length(tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.operations[])
        ) as operation_count,

        splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1])[1] as result_code,

        if (result_code in ('tx_fee_bump_inner_success', 'tx_success'), 1, 0) as successful,

        multiIf(
            tx_envelope.tx.tx.memo.text::Nullable(String) is not null, 'text',
            tx_envelope.tx.tx.memo.hash::Nullable(String) is not null, 'hash',
            tx_envelope.tx.tx.memo.id::Nullable(UInt64) is not null, 'id',
            tx_envelope.tx.tx.memo.return::Nullable(String) is not null, 'return',
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.text::Nullable(String) is not null, 'text',
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.hash::Nullable(String) is not null, 'hash',
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.id::Nullable(UInt64) is not null, 'id',
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.return::Nullable(String) is not null, 'return',
            'none'
        ) as memo_type,

        multiIf(
            tx_envelope.tx.tx.memo.text::Nullable(String) is not null, tx_envelope.tx.tx.memo.text::String,
            tx_envelope.tx.tx.memo.hash::Nullable(String) is not null, tx_envelope.tx.tx.memo.hash::String,
            tx_envelope.tx.tx.memo.id::Nullable(UInt64) is not null, tx_envelope.tx.tx.memo.id::String,
            tx_envelope.tx.tx.memo.return::Nullable(String) is not null, tx_envelope.tx.tx.memo.return::String,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.text::Nullable(String) is not null, tx_envelope.tx.tx.memo.text::String,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.hash::Nullable(String) is not null, tx_envelope.tx.tx.memo.hash::String,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.id::Nullable(UInt64) is not null, tx_envelope.tx.tx.memo.id::String,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.memo.return::Nullable(String) is not null, tx_envelope.tx.tx.memo.return::String,
            null
        ) as memo,

        coalesce(
            tx_envelope.tx.tx.cond.time.min_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx.tx.cond.time_bounds.min_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.time.min_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.time_bounds.min_time::Nullable(DateTime64(3, 'UTC'))
        ) as min_time_bound,

        coalesce(
            tx_envelope.tx.tx.cond.time.max_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx.tx.cond.time_bounds.max_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.time.max_time::Nullable(DateTime64(3, 'UTC')),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.time_bounds.max_time::Nullable(DateTime64(3, 'UTC'))
        ) as max_time_bound,

        coalesce(
            tx_envelope.tx.tx.cond.v2.ledger_bounds.min_ledger::Nullable(UInt32),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.ledger_bounds.min_ledger::Nullable(UInt32)
        ) as min_ledger_bound,

        coalesce(
            tx_envelope.tx.tx.cond.v2.ledger_bounds.max_ledger::Nullable(UInt32),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.ledger_bounds.max_ledger::Nullable(UInt32)
        ) as max_ledger_bound,

        coalesce(
            tx_envelope.tx.tx.cond.v2.min_account_sequence::Nullable(UInt64),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_account_sequence::Nullable(UInt64)
        ) as min_account_sequence,

        coalesce(
            tx_envelope.tx.tx.cond.v2.min_seq_age::Nullable(UInt64),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_seq_age::Nullable(UInt64)
        ) as min_account_sequence_age,

        coalesce(
            tx_envelope.tx.tx.cond.v2.min_seq_ledger_gap::Nullable(UInt32),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_seq_ledger_gap::Nullable(UInt32)
        ) as min_account_sequence_ledger_gap,

        arrayConcat(
            tx_envelope.tx.tx.cond.v2.extra_signers[],
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.extra_signers[]
        ) as extra_signers,

        coalesce(
            tx_envelope.tx.tx.fee::Nullable(UInt64),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.fee::Nullable(UInt64)
        ) as max_fee,

        tx_meta.result.result.fee_charged::UInt64 as fee_charged,
        tx_envelope.tx_fee_bump.tx.fee_source::String as fee_account,
        tx_envelope.tx_fee_bump.tx.fee::UInt64 as new_max_fee,

        coalesce(
            tx_envelope.tx.tx.ext.v1.resource_fee::Nullable(UInt64),
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resource_fee::Nullable(UInt64)
        ) as resource_fee,

        coalesce (
            tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.total_non_refundable_resource_fee_charged::Nullable(UInt64),
            tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.total_non_refundable_resource_fee_charged::Nullable(UInt64)
        ) as non_refundable_resource_fee_charged,

        coalesce(
            tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.total_refundable_resource_fee_charged::Nullable(UInt64),
            tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.total_refundable_resource_fee_charged::Nullable(UInt64)
        ) as refundable_resource_fee_charged,

        coalesce(
            tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.rent_fee_charged::Nullable(UInt64),
            tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.rent_fee_charged::Nullable(UInt64)
        ) as rent_fee_charged,

        non_refundable_resource_fee_charged + refundable_resource_fee_charged as resource_fee_charged,
        max_fee - resource_fee as inclusion_fee,
        fee_charged - resource_fee_charged as inclusion_fee_charged,

        coalesce(
            tx_envelope.tx.tx.ext.v1.resources.instructions::UInt64,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.instructions::UInt64
        ) as soroban_resources_instructions,

        coalesce(
            tx_envelope.tx.tx.ext.v1.resources.disk_read_bytes::UInt64,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.disk_read_bytes::UInt64
        ) as soroban_resources_read_bytes,

        coalesce(
            tx_envelope.tx.tx.ext.v1.resources.write_bytes::UInt64,
            tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.write_bytes::UInt64
        ) as soroban_resources_write_bytes

    from tx_envelopes
    left join tx_metas
    on hash = tx_meta.result.transaction_hash::String
    order by id
)

{{end}}