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

                arrayConcat(
                    tx_meta.tx_apply_processing[],
                    tx_meta.tx_apply_processing.v1.operations[],
                    tx_meta.tx_apply_processing.v2.operations[],
                    tx_meta.tx_apply_processing.v3.operations[],
                    tx_meta.tx_apply_processing.v4.operations[]
                ) as ops_meta
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
                            when 'path_payment_strict_receive' then (
                                'null'::JSON,
                                op_result.op_inner.path_payment_strict_send.success.offers[]::Array(JSON),
                                op_meta.changes[]::Array(JSON)
                            )

                            when 'manage_sell_offer' then (
                                op_result.^op_inner.manage_sell_offer.success.offer::JSON,
                                op_result.op_inner.manage_sell_offer.success.offers_claimed[]::Array(JSON),
                                op_meta.changes[]::Array(JSON)
                            )

                            when 'create_passive_sell_offer' then (
                                'null'::JSON,
                                []::Array(JSON),
                                op_meta.changes[]::Array(JSON)
                            )

                            when 'manage_buy_offer' then (
                                op_result.^op_inner.manage_buy_offer.success.offer::JSON,
                                op_result.op_inner.manage_buy_offer.success.offers_claimed[]::Array(JSON),
                                op_meta.changes[]::Array(JSON)
                            )

                            when 'path_payment_strict_send' then (
                                'null'::JSON,
                                op_result.op_inner.path_payment_strict_send.success.offers[]::Array(JSON),
                                op_meta.changes[]::Array(JSON)
                            )

                            else ('null'::JSON, []::Array(JSON), []::Array(JSON))
                        end,
                        'Tuple(offer JSON, offers_claimed Array(JSON), changes Array(JSON))'
                    )
                ) as t

            from txs
            array join 
                ops as op,
                ops_result as op_result,
                ops_meta as op_meta,
                arrayEnumerate(ops) as op_order
            where result_code in ('success', 'tx_fee_bump_inner_success')
            and type in (
                'path_payment_strict_receive', 
                'manage_sell_offer', 
                'create_passive_sell_offer', 
                'manage_buy_offer', 
                'path_payment_strict_send'
            )
        ),

        trades as (
            select 
                ledger_sequence,
                ledger_closed_at,
                ledger_hash,
                transaction_hash,
                transaction_result_code,
                transaction_source_account,
                id as operation_id,
                order,
                source_account_muxed,
                type,

                firstNonDefault(
                    claim.order_book.asset_sold::Nullable(String),
                    claim.liquidity_pool.asset_sold::Nullable(String),
                    splitByChar('.', JSONAllPaths(claim.^order_book.asset_sold)[1])[1],
                    splitByChar('.', JSONAllPaths(claim.^liquidity_pool.asset_sold)[1])[1]
                ) as selling_asset_type,

                firstNonDefault(
                    claim.order_book.asset_sold.credit_alphanum4.asset_code::String,
                    claim.order_book.asset_sold.credit_alphanum12.asset_code::String,
                    claim.liquidity_pool.asset_sold.credit_alphanum4.asset_code::String,
                    claim.liquidity_pool.asset_sold.credit_alphanum12.asset_code::String
                ) as selling_asset_code,

                firstNonDefault(
                    claim.order_book.asset_sold.credit_alphanum4.issuer::String,
                    claim.order_book.asset_sold.credit_alphanum12.issuer::String,
                    claim.liquidity_pool.asset_sold.credit_alphanum4.issuer::String,
                    claim.liquidity_pool.asset_sold.credit_alphanum12.issuer::String
                ) as selling_asset_issuer,

                firstNonDefault(
                    claim.order_book.amount_sold::String,
                    claim.liquidity_pool.amount_sold::String
                ) as selling_amount,

                firstNonDefault(
                    claim.order_book.asset_bought::Nullable(String),
                    claim.liquidity_pool.asset_bought::Nullable(String),
                    splitByChar('.', JSONAllPaths(claim.^order_book.asset_bought)[1])[1],
                    splitByChar('.', JSONAllPaths(claim.^liquidity_pool.asset_bought)[1])[1]
                ) as buying_asset_type,

                firstNonDefault(
                    claim.order_book.asset_bought.credit_alphanum4.asset_code::String,
                    claim.order_book.asset_bought.credit_alphanum12.asset_code::String,
                    claim.liquidity_pool.asset_bought.credit_alphanum4.asset_code::String,
                    claim.liquidity_pool.asset_bought.credit_alphanum12.asset_code::String
                ) as buying_asset_code,

                firstNonDefault(
                    claim.order_book.asset_bought.credit_alphanum4.issuer::String,
                    claim.order_book.asset_bought.credit_alphanum12.issuer::String,
                    claim.liquidity_pool.asset_bought.credit_alphanum4.issuer::String,
                    claim.liquidity_pool.asset_bought.credit_alphanum12.issuer::String
                ) as buying_asset_issuer,

                firstNonDefault(
                    claim.order_book.amount_bought::String,
                    claim.liquidity_pool.amount_bought::String
                ) as buying_amount,

                firstNonDefault(
                    claim.order_book.seller_id::String
                ) as seller_id,

                firstNonDefault(
                    claim.order_book.offer_id::String
                ) as selling_offer_id,

                if(empty(liquidity_pool_id), 1, 2) as trade_type,

                arrayFirst(
                    x -> liquidity_pool_id is not null and liquidity_pool_id <> '' and firstNonDefault(
                        x.state.data.liquidity_pool.liquidity_pool_id::String,
                        x.updated.data.liquidity_pool.liquidity_pool_id::String
                    ) = liquidity_pool_id
                    ,
                    arrayReverse(t.changes)
                ) as last_pool_change,

                firstNonDefault(
                    claim.liquidity_pool.liquidity_pool_id::String
                ) as liquidity_pool_id,

                firstNonDefault(
                    last_pool_change.state.data.liquidity_pool.body.liquidity_pool_constant_product.params.fee,
                    last_pool_change.updated.data.liquidity_pool.body.liquidity_pool_constant_product.params.fee
                ) as liquidity_pool_fee,

                getSubcolumn(t.offer, 'offer_id') as buying_offer_id

            from ops
            array join 
                t.offers_claimed as claim,
                arrayEnumerate(t.offers_claimed) as order
        )

    select
        * except (last_pool_change)
    from trades
)

{{end}}