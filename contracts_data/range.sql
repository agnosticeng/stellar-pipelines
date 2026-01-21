{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with 
        contracts_data as (
            select 
                JSONExtract(ledger_entry_data, 'Tuple(
                    contract String,
                    durability String,
                    key String,
                    val String
                )') _contract_data_entry,

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
                _contract_data_entry.contract as contract,
                _contract_data_entry.durability as durability,
                _contract_data_entry.key as key,
                _contract_data_entry.val as value
            from iceberg('{{ .ICEBERG_CHANGES_URL_CLICKHOUSE }}', NOSIGN, settings iceberg_use_version_hint=1) as changes
            where ledger_sequence >= {{.RANGE_START}}
            AND ledger_sequence <= {{.RANGE_END}}
            and ledger_entry_type = 'contract_data'
        )

    select 
        columns('^[^_]')
    from contracts_data
)

{{end}}