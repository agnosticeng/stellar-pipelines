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
            tx_processing Array(String),
            upgrades_processing Array(Tuple(
                changes Array(String)
            ))
        )') as _lcm,

        _lcm.ledger_header.header.ledger_seq as ledger_sequence,
        _lcm.ledger_header.header.scp_value.close_time as ledger_close_time,
        _lcm.ledger_header.hash as ledger_hash,
        
        arrayConcat(
            _lcm.tx_set.txs,
            arrayFlatten(_lcm.tx_set.v1.phases.v0.txset_comp_txs_maybe_discounted_fee.txs),
            arrayFlatten(_lcm.tx_set.v1.phases.v1.execution_stages)
        ) as _tx_envelopes_raw,

        _lcm.tx_processing as _tx_result_metas_raw,

        arrayFlatten(
            arrayMap(
                x -> x.changes,
                _lcm.upgrades_processing
            )
        ) as _upgrade_processing_changes_raw

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

                _tx_result_meta.fee_processing as _fee_processing_changes_raw,
                _tx_result_meta.post_tx_apply_fee_processing as _post_tx_apply_fee_processing_raw,
                _tx_meta.tx_changes as _tx_changes_raw,
                _tx_meta.tx_changes_before as _tx_changes_before_raw,
                _tx_meta.tx_changes_after as _tx_changes_after_raw,
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
        )

    select 
        columns('^[^_]'),
        stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id,
        _ops_raw,
        _ops_results_raw,
        _ops_metas_raw,
        _tx_order,
        _fee_processing_changes_raw,
        _post_tx_apply_fee_processing_raw,
        _tx_changes_raw,
        _tx_changes_after_raw,
        _tx_changes_before_raw,

    from tx_envelopes
    left join tx_result_metas
    on tx_envelopes.transaction_hash = tx_result_metas._tx_result_meta.result.transaction_hash
)

{{end}}

{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with
        ops as (
            select 
                columns('^[^_]'),
                stellar_id(ledger_sequence::Int32, _tx_order::Int32, _op_order::Int32) as operation_id,

                JSONExtractKeysAndValues(_op_result_raw, 'String')[1] as _op_result,
                JSONExtractKeysAndValues(_op_result.2, 'String')[1] as _op_result_tr,
                JSONExtractKeysAndValues(_op_result_tr.2, 'String')[1] as _op_result_tr_inner,
                JSONExtractArrayRaw(_op_meta_raw, 'changes') as _op_changes,

                if(
                    JSONType(_op_result_raw) = 'Object', 
                    _op_result.1, 
                    JSONExtractString(_op_result_raw)
                ) as operation_result_code,

                if (
                    JSONType(_op_result_tr.2) = 'Object',
                    _op_result_tr_inner.1,
                    _op_result_tr.2
                ) as operation_inner_result_code
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join 
                _ops_raw as _op_raw,
                _ops_results_raw as _op_result_raw,
                _ops_metas_raw as _op_meta_raw,
                arrayEnumerate(_ops_raw) as _op_order
        ),

        upgrade_processing_changes as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                null as transaction_hash,
                null as transaction_id,
                null as transaction_result_code,
                null as transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'upgrade_processing_changes' as change_source,
                _change_raw
            from ledgers_{{.RANGE_START}}_{{.RANGE_END}}
            array join _upgrade_processing_changes_raw as _change_raw
        ),

        fee_processing_changes as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'fee_processing_changes',
                _change_raw
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join _tx_changes_raw as _change_raw
        ),

        post_tx_apply_fee_processing_changes as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'post_tx_apply_fee_processing_changes',
                _change_raw
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join _post_tx_apply_fee_processing_raw as _change_raw
        ),

        tx_changes as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'tx_changes',
                _change_raw
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join _tx_changes_raw as _change_raw
        ),

        tx_changes_before as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'tx_changes_before',
                _change_raw
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join _tx_changes_before_raw as _change_raw
        ),

        tx_changes_after as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                null as operation_id,
                null as operation_result_code,
                null as operation_inner_result_code,
                'tx_changes_after',
                _change_raw
            from txs_{{.RANGE_START}}_{{.RANGE_END}}
            array join _tx_changes_after_raw as _change_raw
        ),

        ops_changes as (
            select 
                ledger_sequence,
                ledger_close_time,
                ledger_hash,
                transaction_hash,
                transaction_id,
                transaction_result_code,
                transaction_successful,
                operation_id,
                operation_result_code,
                operation_inner_result_code,
                'op_changes',
                _change_raw
            from ops
            array join _op_changes as _change_raw
        ),

        changes as (
            select 
                columns('^[^_]'),

                JSONExtractKeysAndValues(_change_raw, 'String')[1] as _ledger_entry_change,
                
                JSONExtract(_ledger_entry_change.2, 'Tuple(
                    last_modified_ledger_seq UInt32,
                    data String,
                    ext Tuple(
                        v1 Tuple(
                            sponsoring_id String
                        )
                    )
                )') as _ledger_entry,

                JSONExtractKeysAndValues(_ledger_entry.data, 'String')[1] as _ledger_entry_data,

                _ledger_entry_change.1 as type,
                _ledger_entry.last_modified_ledger_seq as last_modified_ledger_sequence,
                _ledger_entry_data.1 as ledger_entry_type,
                _ledger_entry_data.2 as ledger_entry_data                
            from (
                select * from upgrade_processing_changes
                union all 
                select * from fee_processing_changes
                union all 
                select * from post_tx_apply_fee_processing_changes
                union all 
                select * from tx_changes
                union all
                select * from tx_changes_before
                union all 
                select * from tx_changes_after
                union all 
                select * from ops_changes
            )
        )

    select
        columns('^[^_]')
    from changes
)

{{end}}

{{define "check_txs"}}

select 
    throwIf(transaction_result_code = '', 'tx_match_failed') as _
from txs_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "drop_ledgers"}}

drop table ledgers_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "drop_txs"}}

drop table txs_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}
