{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with 
        accounts as (
            select             
                JSONExtract(ledger_entry_data, 'Tuple(
                    account_id String,
                    balance UInt64,
                    seq_num UInt64,
                    num_sub_entries UInt64,
                    inflation_dest String,
                    flags String,
                    home_domain String,
                    thresholds String,
                    ext Tuple(
                        v1 Tuple(
                            liabilities Tuple(
                                buying Int64,
                                selling Int64
                            ),
                            ext Tuple(
                                v2 Tuple(
                                    num_sponsored UInt32,
                                    num_sponsoring UInt32,
                                    ext Tuple(
                                        v3 Tuple(
                                            seq_ledger UInt32,
                                            seq_time UInt64
                                        )
                                    )
                                )
                            )
                        )
                    )
                )') as _account_entry,

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
                changes.source as change_source,
                changes.type as change_type,
                changes.last_modified_ledger_sequence as change_last_modified_ledger_sequence,
                _account_entry.account_id as account_id,
                _account_entry.balance as balance,
                _account_entry.seq_num as sequence_number,
                _account_entry.num_sub_entries as num_sub_entries,
                _account_entry.inflation_dest as inflation_destination,
                _account_entry.flags as flags,
                _account_entry.home_domain as home_domain,
                _account_entry.ext.v1.liabilities.buying as buying_liabilities,
                _account_entry.ext.v1.liabilities.selling as selling_liabilities,
                reinterpretAsUInt8(substring(unhex(_account_entry.thresholds), 1, 1)) as master_weight,
                reinterpretAsUInt8(substring(unhex(_account_entry.thresholds), 2, 1)) as threshold_low,
                reinterpretAsUInt8(substring(unhex(_account_entry.thresholds), 3, 1)) as threshold_medium,
                reinterpretAsUInt8(substring(unhex(_account_entry.thresholds), 4, 1)) as threshold_high,
                _account_entry.ext.v1.ext.v2.num_sponsored as num_sponsored,
                _account_entry.ext.v1.ext.v2.num_sponsoring as num_sponsoring,
                _account_entry.ext.v1.ext.v2.ext.v3.seq_ledger as sequence_ledger,
                _account_entry.ext.v1.ext.v2.ext.v3.seq_time as sequence_time
            from iceberg('{{ .ICEBERG_CHANGES_URL_CLICKHOUSE }}', NOSIGN, settings iceberg_use_version_hint=1) as changes
            where ledger_sequence >= {{.RANGE_START}}
            AND ledger_sequence <= {{.RANGE_END}}
            and ledger_entry_type = 'account'
        )

    select 
        columns('^[^_]')
    from accounts
)

{{end}}


https://stellar-iceberg-testnet.agnostic.tech/changes'