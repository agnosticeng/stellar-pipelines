{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
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
        ),

        ledgers as (
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
                    total_byte_size_of_live_soroban_state UInt64,
                    ext Tuple(
                        v1 Tuple(
                            soroban_fee_write1_kb UInt64
                        )
                    )
                )') as _lcm,

                _lcm.ledger_header.header.ledger_seq as sequence,
                _lcm.ledger_header.header.scp_value.close_time as close_time,
                _lcm.ledger_header.hash as hash,
                _lcm.ledger_header.header.previous_ledger_hash as previous_ledger_hash,

                stellar_id(sequence::Int32, 0::Int32, 0::Int32) as id,

                _lcm.ledger_header.header.total_coins as total_coins,
                _lcm.ledger_header.header.fee_pool as fee_pool,
                _lcm.ledger_header.header.base_fee as base_fee,
                _lcm.ledger_header.header.base_reserve as base_reserve,
                _lcm.ledger_header.header.max_tx_set_size as max_tx_set_size,
                _lcm.ledger_header.header.ledger_version as ledger_version,
                _lcm.ledger_header.header.scp_value.ext.signed.node_id as node_id,
                _lcm.ledger_header.header.scp_value.ext.signed.signature as signature,
                _lcm.total_byte_size_of_live_soroban_state as total_byte_size_of_live_soroban_state,
                _lcm.ext.v1.soroban_fee_write1_kb as soroban_fee_write1_kb,

                arrayConcat(
                    _lcm.tx_set.txs,
                    arrayFlatten(_lcm.tx_set.v1.phases.v0.txset_comp_txs_maybe_discounted_fee.txs),
                    arrayFlatten(_lcm.tx_set.v1.phases.v1.execution_stages)
                ) as _tx_envelopes_raw,

                _lcm.tx_processing as _tx_result_metas_raw,

                arrayMap(
                    x -> JSONExtract(x, 'Tuple(
                        result Tuple(
                            result Tuple(
                                result String,
                            )
                        ),
                        tx_apply_processing String
                    )'),
                    _tx_result_metas_raw
                ) as _tx_results_metas,

                arrayFilter(
                    x -> JSONHas(x.result.result.result, 'tx_success') or JSONHas(x.result.result.result, 'tx_fee_bump_inner_success'),
                    _tx_results_metas
                ) as _successful_tx_results_metas,

                arrayMap(
                    x -> firstNonDefault(
                        JSONExtractArrayRaw(x.tx_apply_processing),
                        JSONExtractArrayRaw(x.tx_apply_processing, 'v1', 'operations'),
                        JSONExtractArrayRaw(x.tx_apply_processing, 'v2', 'operations'),
                        JSONExtractArrayRaw(x.tx_apply_processing, 'v3', 'operations'),
                        JSONExtractArrayRaw(x.tx_apply_processing, 'v4', 'operations')
                    ),
                    _successful_tx_results_metas
                ) as _successful_tx_operations,

                length(_tx_result_metas_raw) as transaction_count,
                length(_successful_tx_results_metas) as successful_transaction_count,
                (transaction_count - successful_transaction_count) as failed_transaction_count,
                arraySum(x -> length(x), _successful_tx_operations) as operation_count,

                arraySum(
                    x -> JSONLength(x, 'tx', 'tx', 'operations') + JSONLength(x, 'tx', 'tx_fee_bump', 'inner_tx', 'tx', 'tx', 'operations'),
                    _tx_envelopes_raw
                ) as tx_set_operation_count
            from galexie
        )

    select 
        sequence,
        close_time,
        hash,
        id,
        previous_ledger_hash,
        total_coins,
        fee_pool,
        base_fee,
        base_reserve,
        max_tx_set_size,
        ledger_version,
        transaction_count,
        successful_transaction_count,
        failed_transaction_count,
        node_id,
        signature,
        total_byte_size_of_live_soroban_state,
        soroban_fee_write1_kb,
        operation_count,
        tx_set_operation_count
    from ledgers
)

{{end}}