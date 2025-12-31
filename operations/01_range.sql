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

{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory as (
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
                        source_account String,
                        operations Array(String)
                    )'
                ) _tx_envelope_inner,

                stellar_hash_transaction(
                    _tx_envelope_raw, 
                    '{{ .NETWORK_PASSPHRASE | default "Public Global Stellar Network ; September 2015" }}'
                ) as transaction_hash,

                if(
                    startsWith(_tx_envelope_inner.source_account, 'M'), 
                    stellar_unmux(_tx_envelope_inner.source_account), 
                    _tx_envelope_inner.source_account
                ) as transaction_source_account,

                if(
                    startsWith(_tx_envelope_inner.source_account, 'M'), 
                    _tx_envelope_inner.source_account,
                    ''
                ) as transaction_source_account_muxed,

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
                )') as _tx_result_meta,

                JSONExtractKeysAndValues(_tx_result_meta.result.result.result, 'String')[1] as _result,

                firstNonDefault(
                    JSONExtractArrayRaw(_result.2),
                    JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_success'),
                    JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_failed')
                ) as _ops_results_raw,

                _tx_order,

                if(
                    JSONType(_tx_result_meta.result.result.result) = 'Object', 
                    _result.1, 
                    _tx_result_meta.result.result.result
                ) as transaction_result_code,

                (transaction_result_code in ('tx_fee_bump_inner_success', 'tx_success')) as transaction_successful
            from ledgers_{{.RANGE_START}}_{{.RANGE_END}} 
            array join 
                _tx_result_metas_raw as _tx_result_meta_raw,
                arrayEnumerate(_tx_result_metas_raw) as _tx_order
        ),

        txs as (
            select 
                columns('^[^_]'),
                stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id,
                _ops_raw,
                _ops_results_raw,
                _tx_order
            from tx_envelopes
            left join tx_result_metas
            on tx_envelopes.transaction_hash = tx_result_metas._tx_result_meta.result.transaction_hash
        ),

        ops as (
            select 
                columns('^[^_]'),

                JSONExtractString(_op_raw, 'source_account') as _source_account,
                JSONExtractRaw(_op_raw, 'body') as _body,
                JSONExtractKeysAndValues(_body, 'String')[1] as _body_inner,
                JSONExtractKeysAndValues(_op_result_raw, 'String')[1] as _op_result,
                JSONExtractKeysAndValues(_op_result.2, 'String')[1] as _op_result_tr,
                JSONExtractKeysAndValues(_op_result_tr.2, 'String')[1] as _op_result_tr_inner,

                stellar_id(ledger_sequence::Int32, _tx_order::Int32, _op_order::Int32) as id,
                _body_inner.1 as type,
                _body_inner.2 as body,

                if(
                    startsWith(_source_account, 'M'), 
                    stellar_unmux(_source_account), 
                    _source_account
                ) as source_account,

                if(
                    startsWith(_source_account, 'M'), 
                    _source_account,
                    ''
                ) as source_account_muxed,

                if(
                    JSONType(_op_result_raw) = 'Object', 
                    _op_result.1, 
                    JSONExtractString(_op_result_raw)
                ) as result_code,

                if (
                    JSONType(_op_result_tr.2) = 'Object',
                    _op_result_tr_inner.1,
                    _op_result_tr.2
                ) as inner_result_code,

                _op_result_tr_inner.2 as result_body
            from txs
            array join 
                _ops_raw as _op_raw,
                _ops_results_raw as _op_result_raw,
                arrayEnumerate(_ops_raw) as _op_order
        )

    select
        ledger_sequence,        
        ledger_close_time,         
        ledger_hash,    
        transaction_hash,           
        transaction_id,
        transaction_result_code,
        transaction_successful,
        id,    

        firstNonDefault(
            source_account,
            transaction_source_account
        ) as source_account,

        firstNonDefault(
            source_account_muxed,
            transaction_source_account_muxed
        ) as source_account_muxed,

        type,                     
        body,                    
        result_code,              
        inner_result_code,       
        result_body
    from ops
)

{{end}}

{{define "drop_ledgers"}}

drop table ledgers_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "check_txs"}}

select 
    throwIf(transaction_result_code = '', 'tx_match_failed') as _
from range_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}