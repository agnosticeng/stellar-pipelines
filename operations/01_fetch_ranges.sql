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
                )::Array(JSON) as ops,

                arrayConcat(
                    tx_meta.result.result.result.tx_success[],
                    tx_meta.result.result.result.tx_failed[],
                    tx_meta.result.result.result.tx_fee_bump_inner_success.result.result.tx_success[],
                    tx_meta.result.result.result.tx_fee_bump_inner_failed.result.result.tx_failed[]
                )::Array(JSON) as ops_result,
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
                result_code as transaction_result_code,
                source_account as transaction_source_account,
                stellar_id(ledger_sequence::Int32, tx_order::Int32, op_order::Int32) as id,
                op.source_account::String as source_account_muxed,

                firstNonDefault(
                    splitByChar('.', JSONAllPaths(op.^body)[1])[1],
                    op.body::String
                ) as type,
        
                untuple(
                    cast(
                        case type
                            when 'create_account' then (
                                op.^body.create_account,
                                firstNonDefault(
                                    op_result.op_inner.create_account, 
                                    op_result.^op_inner.create_account
                                )
                            )

                            when 'payment' then (
                                op.^body.payment,
                                firstNonDefault(
                                    op_result.op_inner.payment, 
                                    op_result.^op_inner.payment
                                )
                            )

                            when 'path_payment_strict_receive' then (
                                op.^body.path_payment_strict_receive,
                                firstNonDefault(
                                    op_result.op_inner.path_payment_strict_receive, 
                                    op_result.^op_inner.path_payment_strict_receive
                                )
                            )

                            when 'manage_sell_offer' then (
                                op.^body.manage_sell_offer,
                                firstNonDefault(
                                    op_result.op_inner.manage_sell_offer,
                                    op_result.^op_inner.manage_sell_offer
                                )
                            )

                            when 'create_passive_sell_offer' then (
                                op.^body.create_passive_sell_offer,
                                firstNonDefault(
                                    op_result.op_inner.create_passive_sell_offer,
                                    op_result.^op_inner.create_passive_sell_offer
                                )
                            )

                            when 'set_options' then (
                                op.^body.set_options,
                                firstNonDefault(
                                    op_result.op_inner.set_options,
                                    op_result.^op_inner.set_options
                                )
                            )

                            when 'change_trust' then (
                                op.^body.change_trust,
                                firstNonDefault(
                                    op_result.op_inner.change_trust,
                                    op_result.^op_inner.change_trust
                                )
                            )
                        
                            when 'allow_trust' then (
                                op.^body.allow_trust,
                                firstNonDefault(
                                    op_result.op_inner.allow_trust,
                                    op_result.^op_inner.allow_trust
                                )
                            )

                            when 'account_merge' then (
                                op.^body.account_merge,
                                firstNonDefault(
                                    op_result.op_inner.account_merge,
                                    op_result.^op_inner.account_merge
                                )
                            )

                            when 'inflation' then (
                                op.^body.inflation,
                                firstNonDefault(
                                    op_result.op_inner.inflation,
                                    op_result.^op_inner.inflation
                                )
                            )

                            when 'manage_data' then (
                                op.^body.manage_data,
                                firstNonDefault(
                                    op_result.op_inner.manage_data,
                                    op_result.^op_inner.manage_data
                                )
                            )

                            when 'bump_sequence' then (
                                op.^body.bump_sequence,
                                firstNonDefault(
                                    op_result.op_inner.bump_sequence,
                                    op_result.^op_inner.bump_sequence
                                )
                            )

                            when 'manage_buy_offer' then (
                                op.^body.manage_buy_offer,
                                firstNonDefault(
                                    op_result.op_inner.manage_buy_offer,
                                    op_result.^op_inner.manage_buy_offer
                                )
                            )

                            when 'path_payment_strict_send' then (
                                op.^body.path_payment_strict_send,
                                firstNonDefault(
                                    op_result.op_inner.path_payment_strict_send,
                                    op_result.^op_inner.path_payment_strict_send
                                )
                            )

                            when 'create_claimable_balance' then (
                                op.^body.create_claimable_balance,
                                firstNonDefault(
                                    op_result.op_inner.create_claimable_balance,
                                    op_result.^op_inner.create_claimable_balance
                                )
                            )

                            when 'claim_claimable_balance' then (
                                op.^body.claim_claimable_balance,
                                firstNonDefault(
                                    op_result.op_inner.claim_claimable_balance,
                                    op_result.^op_inner.claim_claimable_balance
                                )
                            )

                            when 'begin_sponsoring_future_reserves' then (
                                op.^body.begin_sponsoring_future_reserves,
                                firstNonDefault(
                                    op_result.op_inner.begin_sponsoring_future_reserves,
                                    op_result.^op_inner.begin_sponsoring_future_reserves
                                )
                            )

                            when 'end_sponsoring_future_reserves' then (
                                op.^body.end_sponsoring_future_reserves,
                                firstNonDefault(
                                    op_result.op_inner.end_sponsoring_future_reserves,
                                    op_result.^op_inner.end_sponsoring_future_reserves
                                )
                            )

                            when 'revoke_sponsorship' then (
                                op.^body.revoke_sponsorship,
                                firstNonDefault(
                                    op_result.op_inner.revoke_sponsorship,
                                    op_result.^op_inner.revoke_sponsorship
                                )
                            )

                            when 'clawback' then (
                                op.^body.clawback,
                                firstNonDefault(
                                    op_result.op_inner.clawback,
                                    op_result.^op_inner.clawback
                                )
                            )

                            when 'clawback_claimable_balance' then (
                                op.^body.clawback_claimable_balance,
                                firstNonDefault(
                                    op_result.op_inner.clawback_claimable_balance,
                                    op_result.^op_inner.clawback_claimable_balance
                                )
                            )

                            when 'set_trust_line_flags' then (
                                op.^body.set_trust_line_flags,
                                firstNonDefault(
                                    op_result.op_inner.set_trust_line_flags,
                                    op_result.^op_inner.set_trust_line_flags
                                )
                            )

                            when 'liquidity_pool_deposit' then (
                                op.^body.liquidity_pool_deposit,
                                firstNonDefault(
                                    op_result.op_inner.liquidity_pool_deposit,
                                    op_result.^op_inner.liquidity_pool_deposit
                                )
                            )

                            when 'liquidity_pool_withdraw' then (
                                op.^body.liquidity_pool_withdraw,
                                firstNonDefault(
                                    op_result.op_inner.liquidity_pool_withdraw,
                                    op_result.^op_inner.liquidity_pool_withdraw
                                )
                            )

                            when 'invoke_host_function' then (
                                op.^body.invoke_host_function,
                                firstNonDefault(
                                    op_result.op_inner.invoke_host_function,
                                    op_result.^op_inner.invoke_host_function
                                )
                            )

                            when 'extend_footprint_ttl' then (
                                op.^body.extend_footprint_ttl,
                                firstNonDefault(
                                    op_result.op_inner.extend_footprint_ttl,
                                    op_result.^op_inner.extend_footprint_ttl
                                )
                            )

                            when 'restore_footprint' then (
                                op.^body.restore_footprint,
                                firstNonDefault(
                                    op_result.op_inner.restore_footprint,
                                    op_result.^op_inner.restore_footprint
                                )
                            )

                            else ('null'::JSON, throwIf(1, 'unhandled op type'))
                        end,
                        'Tuple(body JSON, result Dynamic)'
                    )
                ) as t
            from txs
            array join 
                ops as op,
                ops_result as op_result,
                arrayEnumerate(ops) as op_order
        )

    select
        ledger_sequence,
        ledger_closed_at,
        ledger_hash,
        transaction_hash,
        transaction_result_code,
        transaction_source_account,
        id,
        source_account_muxed,
        type,
        t.body as body,
        t.result as result
    from ops
)

{{end}}