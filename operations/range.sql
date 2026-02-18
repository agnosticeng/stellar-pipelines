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
                    tx_v0 Tuple(tx String),
                    tx Tuple(tx String),
                    tx_fee_bump Tuple(
                        tx Tuple(
                            inner_tx Tuple(
                                tx Tuple(tx String)
                            )
                        )
                    )
                )') as _tx_envelope,

                JSONExtract(
                    firstNonDefault(
                        _tx_envelope.tx_v0.tx,
                        _tx_envelope.tx.tx,
                        _tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx
                    ),
                    'Tuple(
                        source_account_ed25519 String,
                        source_account String,
                        operations Array(String)
                    )'
                ) _tx_envelope_inner,

                multiIf(
                    length(_tx_envelope_inner.source_account) > 0, _tx_envelope_inner.source_account,
                    length(_tx_envelope_inner.source_account_ed25519) > 0, stellar_uint256_to_account(_tx_envelope_inner.source_account_ed25519),
                    ''
                ) as _transaction_source_account,

                if(
                    startsWith(_transaction_source_account, 'M'),
                    stellar_unmux(_transaction_source_account),
                    _transaction_source_account
                ) as transaction_source_account,

                if(
                    startsWith(_transaction_source_account, 'M'),
                    _transaction_source_account,
                    ''
                ) as transaction_source_account_muxed,

                _tx_envelope_inner.operations as _ops_raw,

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
                _tx_result_meta.result.transaction_hash as transaction_hash,

                if(
                    JSONType(_tx_result_meta.result.result.result) = 'Object',
                    _result.1,
                    _tx_result_meta.result.result.result
                ) as transaction_result_code,

                (transaction_result_code in ('tx_fee_bump_inner_success', 'tx_success')) as transaction_successful,

                stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id,
                _ops_raw,
                _ops_results_raw,
                _tx_order
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

        multiIf(
            source_account <> '', source_account_muxed,
            transaction_source_account <> '', transaction_source_account_muxed,
            ''
        ) as source_account_muxed,

        type,
        body,
        result_code,
        inner_result_code,
        result_body
    from ops
)

{{end}}
