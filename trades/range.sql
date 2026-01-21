{{define "create_ledgers"}}

create table ledgers_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory as (
    with 
        galexie as (
            select 
                ledger_close_meta
            from executable(
                'ch-stellar table-function galexie',
                ArrowStream,
                'ledger_close_meta String',
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
        )
        
    select            
        firstNonDefault(
            JSONExtractString(ledger_close_meta, 'v0'),
            JSONExtractString(ledger_close_meta, 'v1'),
            JSONExtractString(ledger_close_meta, 'v2')
        ) as _lcm_raw,

        JSONExtract(_lcm_raw, ' Tuple(
            ledger_header Tuple(
                hash String,
                header Tuple(
                    ledger_seq Int32,
                    scp_value Tuple(
                        close_time DateTime64(6, \'UTC\')
                    )
                )
            ),
            tx_set Tuple(
                txs Array(String),
                v1 Tuple(
                    phases Array(Tuple(
                        v0 Array(Tuple(
                            txset_comp_txs_maybe_discounted_fee Tuple(
                                txs Array(String)
                            )
                        )),
                        v1 Tuple(
                            execution_stages Array(Array(Array(String)))
                        )
                    ))
                )
            ),
            tx_processing Array(String)
        )') as _lcm,

        _lcm.ledger_header.header.ledger_seq as ledger_sequence,
        _lcm.ledger_header.header.scp_value.close_time as ledger_close_time,
        _lcm.ledger_header.hash as ledger_hash,
        
        arrayConcat(
            _lcm.tx_set.txs,
            arrayFlatten(_lcm.tx_set.v1.phases.v0.txset_comp_txs_maybe_discounted_fee.txs),
            arrayFlatten(_lcm.tx_set.v1.phases.v1.execution_stages)
        ) as _tx_envelopes_raw,

        _lcm.tx_processing as _tx_result_metas_raw

    from galexie
)

{{end}}


{{define "create_txs"}}

create table txs_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory as (
    with
        tx_envelopes as (
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

                stellar_hash_transaction(
                    _tx_envelope_raw, 
                    '{{ .NETWORK_PASSPHRASE | default "Public Global Stellar Network ; September 2015" }}'
                ) as transaction_hash,

                _tx_envelope_inner.operations as _ops_raw
            from ledgers_{{.RANGE_START}}_{{.RANGE_END}} 
            array join 
                _tx_envelopes_raw as _tx_envelope_raw
        ),

        tx_result_metas as (
            select             
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

                if(
                    JSONType(_tx_result_meta.result.result.result) = 'Object', 
                    _result.1, 
                    _tx_result_meta.result.result.result
                ) as _transaction_result_code,

                (_transaction_result_code in ('tx_fee_bump_inner_success', 'tx_success')) as _transaction_successful
            from ledgers_{{.RANGE_START}}_{{.RANGE_END}} 
            array join 
                _tx_result_metas_raw as _tx_result_meta_raw,
                arrayEnumerate(_tx_result_metas_raw) as _tx_order
        )

    select 
        columns('^[^_]'),
        stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id,
        _transaction_result_code,
        _transaction_successful,
        _ops_raw,
        _ops_results_raw,
        _ops_metas_raw,
        _tx_order
    from tx_envelopes
    left join tx_result_metas
    on tx_envelopes.transaction_hash = tx_result_metas._tx_result_meta.result.transaction_hash
)

{{end}}

{{define "drop_ledgers"}}

drop table ledgers_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory as (
    with
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
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
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

{{define "check_txs"}}

select 
    throwIf(_transaction_result_code = '', 'tx_match_failed') as _
from txs_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "drop_txs"}}

drop table txs_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}