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
                            fee_source String,
                            fee Int64,
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
                        fee Int64,
                        seq_num Int64,
                        cond Tuple(
                            time_bounds Tuple(
                                min_time Nullable(DateTime64(6, \'UTC\')),
                                max_time Nullable(DateTime64(6, \'UTC\'))
                            ),
                            v2 Tuple(
                                time_bounds Tuple(
                                    min_time Nullable(DateTime64(6, \'UTC\')),
                                    max_time Nullable(DateTime64(6, \'UTC\'))
                                ),
                                ledger_bounds Tuple(
                                    min_ledger Nullable(UInt32),
                                    max_ledger Nullable(UInt32)
                                ),
                                min_seq_num Nullable(Int64),
                                min_seq_age Nullable(UInt64),
                                min_seq_ledger_gap UInt32,
                                extra_signers Array(String)
                            )
                        ),
                        memo String,
                        operations Array(String),
                        ext Tuple(
                            v1 Tuple(
                                resources Tuple(
                                    instructions UInt32,
                                    disk_read_bytes UInt32,
                                    write_bytes UInt32
                                ),
                                resource_fee Int64
                            )
                        )
                    )'
                ) _tx_envelope_inner,

                stellar_hash_transaction(
                    _tx_envelope_raw, 
                    '{{ .NETWORK_PASSPHRASE | default "Public Global Stellar Network ; September 2015" }}'
                ) as hash,

                _tx_envelope_inner.source_account as source_account,
                _tx_envelope_inner.seq_num as account_sequence,
                length(_tx_envelope_inner.operations) as operation_count,

                multiIf(
                    JSONHas(_tx_envelope_inner.memo, 'text'), 'text',
                    JSONHas(_tx_envelope_inner.memo, 'hash'), 'hash',
                    JSONHas(_tx_envelope_inner.memo, 'id'), 'id',
                    JSONHas(_tx_envelope_inner.memo, 'return'), 'return',
                    'none'
                ) as memo_type,

                multiIf(
                    memo_type = 'text', JSONExtractString(_tx_envelope_inner.memo, 'text'),
                    memo_type = 'hash', JSONExtractString(_tx_envelope_inner.memo, 'hash'),
                    memo_type = 'id', JSONExtractString(_tx_envelope_inner.memo, 'id'),
                    memo_type = 'return', JSONExtractString(_tx_envelope_inner.memo, 'return'),
                    null
                ) as memo,

                firstNonDefault(
                    _tx_envelope_inner.cond.time_bounds.min_time,
                    _tx_envelope_inner.cond.v2.time_bounds.min_time
                ) as min_time_bound,

                firstNonDefault(
                    _tx_envelope_inner.cond.time_bounds.max_time,
                    _tx_envelope_inner.cond.v2.time_bounds.max_time
                ) as max_time_bound,

                _tx_envelope_inner.cond.v2.ledger_bounds.min_ledger as min_ledger_bound,
                _tx_envelope_inner.cond.v2.ledger_bounds.max_ledger as max_ledger_bound,
                _tx_envelope_inner.cond.v2.min_seq_num as min_account_sequence,
                _tx_envelope_inner.cond.v2.min_seq_age as min_account_sequence_age,
                _tx_envelope_inner.cond.v2.min_seq_ledger_gap as min_account_sequence_ledger_gap,
                _tx_envelope_inner.cond.v2.extra_signers as extra_signers,
                _tx_envelope_inner.fee as max_fee,
                _tx_envelope.tx_fee_bump.tx.fee_source as fee_source,
                _tx_envelope.tx_fee_bump.tx.fee as new_max_fee,
                _tx_envelope_inner.ext.v1.resource_fee as resource_fee,
                _tx_envelope_inner.ext.v1.resources.instructions as soroban_resources_instructions,
                _tx_envelope_inner.ext.v1.resources.disk_read_bytes as soroban_resources_disk_read_bytes,
                _tx_envelope_inner.ext.v1.resources.write_bytes as soroban_resources_write_bytes
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
                            fee_charged Int64,
                            result String,
                        )
                    ),
                    tx_apply_processing Tuple(
                        v3 String,
                        v4 String
                    )
                )') as _tx_result_meta,

                firstNonDefault(
                    _tx_result_meta.tx_apply_processing.v3,
                    _tx_result_meta.tx_apply_processing.v4
                ) as _tx_meta_raw,

                JSONExtract(_tx_meta_raw, 'Tuple(
                    soroban_meta Tuple(
                        ext Tuple(
                            v1 Tuple(
                                total_non_refundable_resource_fee_charged Int64,
                                total_refundable_resource_fee_charged Int64,
                                rent_fee_charged Int64
                            )
                        )
                    )     
                )') as _tx_meta,

                _tx_result_meta.result.transaction_hash as hash,
                tx_order,

                firstNonDefault(
                    JSONExtractString(_tx_result_meta.result.result.result, 'tx_fee_bump_inner_success', 'transaction_hash'),
                    JSONExtractString(_tx_result_meta.result.result.result, 'tx_fee_bump_inner_failed', 'transaction_hash')
                ) as inner_transaction_hash,

                JSONExtractKeysAndValues(_tx_result_meta.result.result.result, 'String')[1] as _result,

                if(
                    JSONType(_tx_result_meta.result.result.result) = 'Object', 
                    _result.1, 
                    _tx_result_meta.result.result.result
                ) as result_code,

                (result_code in ('tx_fee_bump_inner_success', 'tx_success')) as successful,

                _tx_result_meta.result.result.fee_charged as fee_charged,
                _tx_meta.soroban_meta.ext.v1.total_non_refundable_resource_fee_charged as total_non_refundable_resource_fee_charged,
                _tx_meta.soroban_meta.ext.v1.total_refundable_resource_fee_charged as total_refundable_resource_fee_charged,
                _tx_meta.soroban_meta.ext.v1.rent_fee_charged as rent_fee_charged
            from ledgers_{{.RANGE_START}}_{{.RANGE_END}}  
            array join 
                _tx_result_metas_raw as _tx_result_meta_raw,
                arrayEnumerate(_tx_result_metas_raw) as tx_order
        ),

        txs as (
            select
                columns('^[^_]'),
                stellar_id(ledger_sequence::Int32, tx_order::Int32, 0::Int32) as id,

                if(
                    startsWith(source_account, 'M'), 
                    stellar_unmux(source_account), 
                    source_account
                ) as account,

                if(
                    startsWith(source_account, 'M'), 
                    source_account,
                    ''
                ) as account_muxed,

                if(
                    startsWith(fee_source, 'M'), 
                    stellar_unmux(fee_source), 
                    fee_source
                ) as fee_account,

                if(
                    startsWith(fee_source, 'M'), 
                    fee_source,
                    ''
                ) as fee_account_muxed,

                total_non_refundable_resource_fee_charged + total_refundable_resource_fee_charged as resource_fee_charged,
                max_fee - resource_fee as inclusion_fee,
                fee_charged - resource_fee_charged as inclusion_fee_charged
            from tx_envelopes
            left join tx_result_metas
            on tx_envelopes.hash = tx_result_metas.hash
            order by id
        )

    select 
        ledger_sequence,
        ledger_close_time,
        ledger_hash,
        hash,
        id,
        inner_transaction_hash,
        account,
        account_muxed,
        account_sequence,
        fee_account,
        fee_account_muxed,
        operation_count,
        result_code,
        successful,
        memo_type,
        memo,
        min_time_bound,
        max_time_bound,
        min_ledger_bound,
        max_ledger_bound,
        min_account_sequence,
        min_account_sequence_age,
        min_account_sequence_ledger_gap,
        extra_signers,
        max_fee,
        fee_charged,
        new_max_fee,
        resource_fee,
        total_non_refundable_resource_fee_charged,
        total_refundable_resource_fee_charged,
        rent_fee_charged,
        resource_fee_charged,
        inclusion_fee,
        inclusion_fee_charged,
        soroban_resources_instructions,
        soroban_resources_disk_read_bytes,
        soroban_resources_write_bytes
    from txs
)

{{end}}

{{define "drop_ledgers"}}

drop table ledgers_{{.RANGE_START}}_{{.RANGE_END}} 

{{end}}

{{define "check_txs"}}

select 
    throwIf(result_code = '', 'tx_match_failed') as _
from range_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}