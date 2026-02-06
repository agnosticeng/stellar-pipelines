{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
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

                _ledger.ledger_header.header.ledger_seq as sequence,
                _ledger.ledger_header.header.scp_value.close_time as close_time,
                _ledger.ledger_header.hash as hash,
                _ledger.ledger_header.header.previous_ledger_hash as previous_ledger_hash,

                stellar_id(sequence::Int32, 0::Int32, 0::Int32) as id,

                _ledger.ledger_header.header.total_coins as total_coins,
                _ledger.ledger_header.header.fee_pool as fee_pool,
                _ledger.ledger_header.header.base_fee as base_fee,
                _ledger.ledger_header.header.base_reserve as base_reserve,
                _ledger.ledger_header.header.max_tx_set_size as max_tx_set_size,
                _ledger.ledger_header.header.ledger_version as ledger_version,
                _ledger.ledger_header.header.scp_value.ext.signed.node_id as node_id,
                _ledger.ledger_header.header.scp_value.ext.signed.signature as signature,
                _ledger.total_byte_size_of_live_soroban_state as total_byte_size_of_live_soroban_state,
                _ledger.ext.v1.soroban_fee_write1_kb as soroban_fee_write1_kb,

                _ledger.tx_set as _tx_envelopes_raw,
                _ledger.tx_processing as _tx_result_metas_raw,

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
