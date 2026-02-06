{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory as (
    with
        galexie as (
            select
                ledger
            from executable(
                'ch-stellar table-function galexie --normalized',
                ArrowStream,
                'ledger String',
                (
                    select
                        '{{ .GALEXIE_URL }}' as url,
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
                JSONExtract(ledger, 'Tuple(
                    ledger_header Tuple(
                        hash String,
                        header Tuple(
                            ledger_seq Int32,
                            previous_ledger_hash String,
                            total_coins UInt64,
                            fee_pool UInt64,
                            base_fee UInt64,
                            base_reserve UInt64,
                            max_tx_set_size UInt32,
                            ledger_version UInt32,
                            scp_value Tuple(
                                close_time DateTime64(6, \'UTC\'),
                                ext Tuple(
                                    signed Tuple(
                                        node_id String,
                                        signature String
                                    )
                                )
                            )
                        )
                    ),
                    tx_set Array(String),
                    tx_processing Array(String),
                    total_byte_size_of_live_soroban_state UInt64,
                    ext Tuple(
                        v1 Tuple(
                            soroban_fee_write1_kb UInt64
                        )
                    )
                )') as _ledger,

                _ledger.ledger_header.header.ledger_seq as ledger_sequence,
                _ledger.ledger_header.header.scp_value.close_time as ledger_close_time,
                _ledger.ledger_header.hash as ledger_hash,

                _ledger.tx_set as _tx_envelopes_raw,
                _ledger.tx_processing as _tx_result_metas_raw
            from galexie
        ),

        txs as (
            select
                columns('^[^_]'),


                JSONExtract(_tx_envelope_raw, 'Tuple(
                    tx Tuple(
                        tx String
                    ),
                    tx_fee_bump Tuple(
                        tx Tuple(
                            inner_tx Tuple(
                                tx Tuple(
                                    tx String
                                )
                            )
                        )
                    )
                )') as _tx_envelope,

                JSONExtract(
                    firstNonDefault(
                        _tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx,
                        _tx_envelope.tx.tx
                    ),
                    'Tuple(
                        operations Array(String)
                    )'
                ) _tx_envelope_inner,

                _tx_envelope_inner.operations as _ops_raw,

                JSONExtract(_tx_result_meta_raw, 'Tuple(
                    result Tuple(
                        transaction_hash String,
                        result Tuple(
                            result String,
                        )
                    ),
                    tx_apply_processing Tuple(
                        operations Array(String),
                        v1 String,
                        v2 String,
                        v3 String,
                        v4 String
                    ),
                    fee_processing Array(String),
                    post_tx_apply_fee_processing Array(String)
                )') as _tx_result_meta,

                JSONExtractKeysAndValues(_tx_result_meta.result.result.result, 'String')[1] as _result,

                firstNonDefault(
                    JSONExtractArrayRaw(_result.2),
                    JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_success'),
                    JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_failed')
                ) as _ops_results_raw,

                firstNonDefault(
                    _tx_result_meta.tx_apply_processing.v1,
                    _tx_result_meta.tx_apply_processing.v2,
                    _tx_result_meta.tx_apply_processing.v3,
                    _tx_result_meta.tx_apply_processing.v4
                ) as _tx_meta_raw,

                JSONExtract(_tx_meta_raw, 'Tuple(
                    tx_changes Array(String),
                    tx_changes_before Array(String),
                    tx_changes_after Array(String),
                    operations Array(String)
                )') as _tx_meta,

                firstNonDefault(
                    _tx_result_meta.tx_apply_processing.operations,
                    _tx_meta.operations
                ) as _ops_metas_raw,

                _tx_order,
                _tx_result_meta.result.transaction_hash as transaction_hash,

                if(
                    JSONType(_tx_result_meta.result.result.result) = 'Object',
                    _result.1,
                    _tx_result_meta.result.result.result
                ) as _transaction_result_code,

                (_transaction_result_code in ('tx_fee_bump_inner_success', 'tx_success')) as _transaction_successful,

                stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id
            from ledgers
            array join
                _tx_envelopes_raw as _tx_envelope_raw,
                _tx_result_metas_raw as _tx_result_meta_raw,
                arrayEnumerate(_tx_result_metas_raw) as _tx_order
        ),

        ops as (
            select
                columns('^[^_]'),

                JSONExtractString(_op_raw, 'source_account') as _source_account,
                JSONExtractRaw(_op_raw, 'body') as _body_raw,
                JSONExtractKeysAndValues(_body_raw, 'String')[1] as _body_inner,
                JSONExtractKeysAndValues(_op_result_raw, 'String')[1] as _op_result,
                JSONExtractKeysAndValues(_op_result.2, 'String')[1] as _op_result_tr,
                JSONExtractKeysAndValues(_op_result_tr.2, 'String')[1] as _op_result_tr_inner,

                stellar_id(ledger_sequence::Int32, _tx_order::Int32, _op_order::Int32) as operation_id,
                _body_inner.1 as operation_type,
                _body_inner.2 as _body,

                if(
                    JSONType(_op_result_raw) = 'Object',
                    _op_result.1,
                    JSONExtractString(_op_result_raw)
                ) as _result_code,

                if (
                    JSONType(_op_result_tr.2) = 'Object',
                    _op_result_tr_inner.1,
                    _op_result_tr.2
                ) as _inner_result_code,

                _op_result_tr_inner.2 as _result_body,

                JSONExtractRaw(_result_body, 'offer') as _offer_raw,
                JSONExtractKeysAndValues(_offer_raw, 'String')[1] as _offer_raw_inner,

                if (
                    JSONType(_offer_raw) = 'Object',
                    _offer_raw_inner.1,
                    JSONExtractString(_offer_raw)
                ) as _offer_action,

                _offer_raw_inner.2 as _offer_body_raw,

                JSONExtractArrayRaw(_op_meta_raw, 'changes') as _changes_raw,
                firstNonDefault(
                    JSONExtractArrayRaw(_result_body, 'offers'),
                    JSONExtractArrayRaw(_result_body, 'offers_claimed')
                ) as _offers_claimed_raw
            from txs
            array join
                _ops_raw as _op_raw,
                _ops_results_raw as _op_result_raw,
                _ops_metas_raw as _op_meta_raw,
                arrayEnumerate(_ops_raw) as _op_order
            where _transaction_successful = 1
            and _inner_result_code = 'success'
            and operation_type in (
                'path_payment_strict_receive',
                'manage_sell_offer',
                'create_passive_sell_offer',
                'manage_buy_offer',
                'path_payment_strict_send'
            )
        ),

        trades as (
            select
                columns('^[^_]'),
                JSONExtractKeysAndValues(_offer_claimed_raw, 'String')[1] as _offer_claimed_raw_inner,
                _offer_claimed_raw_inner.1 as _offer_claimed_type,
                _offer_claimed_raw_inner.2 as _offer_claimed_body_raw,
                JSONExtractString(_offer_claimed_body_raw, 'asset_sold') as _asset_sold,
                JSONExtractString(_offer_claimed_body_raw, 'asset_bought') as _asset_bought,
                JSONExtractKeysAndValues(_asset_sold, 'String')[1] as _asset_type_and_data_sold,
                JSONExtractKeysAndValues(_asset_bought, 'String')[1] as _asset_type_and_data_bought,

                _offer_claimed_type as trade_type,
                order,

                if(
                    JSONType(_asset_sold) = 'Object',
                    _asset_type_and_data_sold.1,
                    _asset_sold
                ) as selling_asset_type,

                JSONExtractString(_asset_type_and_data_sold.2, 'asset_code') as selling_asset_code,
                JSONExtractString(_asset_type_and_data_sold.2, 'issuer') as selling_asset_issuer,
                stellar_asset_id(selling_asset_code, selling_asset_issuer, selling_asset_type) as selling_asset_id,
                JSONExtractInt(_offer_claimed_body_raw, 'amount_sold') as selling_amount,

                if(
                    JSONType(_asset_bought) = 'Object',
                    _asset_type_and_data_bought.1,
                    _asset_bought
                ) as buying_asset_type,

                JSONExtractString(_asset_type_and_data_bought.2, 'asset_code') as buying_asset_code,
                JSONExtractString(_asset_type_and_data_bought.2, 'issuer') as buying_asset_issuer,
                stellar_asset_id(buying_asset_code, buying_asset_issuer, buying_asset_type) as buying_asset_id,
                JSONExtractInt(_offer_claimed_body_raw, 'amount_bought') as buying_amount,

                JSONExtractString(_offer_claimed_body_raw, 'seller_id') as seller_id,
                JSONExtractString(_offer_claimed_body_raw, 'offer_id') as selling_offer_id,
                JSONExtractString(_offer_claimed_body_raw, 'liquidity_pool_id') as liquidity_pool_id,

                arrayLast(
                    x -> JSONExtractString(x, 'liquidity_pool_id') = liquidity_pool_id,
                    arrayMap(
                        x -> JSONExtractRaw(x, 'state', 'data', 'liquidity_pool'),
                        _changes_raw
                    )
                ) as _last_pool_change,

                JSONExtractInt(_last_pool_change, 'body', 'liquidity_pool_constant_product', 'params', 'fee') as liquidity_pool_fee,
                JSONExtractString(_offer_body_raw, 'offer_id') as buying_offer_id
            from ops
            array join
                _offers_claimed_raw as _offer_claimed_raw,
                arrayEnumerate(_offers_claimed_raw) as order
        )

    select
        columns('^[^_]')
    from trades
)

{{end}}
