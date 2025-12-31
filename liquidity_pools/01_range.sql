{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with 
        liquidity_pools as (
            select 
                JSONExtract(ledger_entry_data, 'Tuple(
                    liquidity_pool_id String,
                    body String
                )') as _liquidity_pool_entry,

                JSONExtractKeysAndValues(_liquidity_pool_entry.body, 'String')[1] as _liquidity_pool_entry_body,

                JSONExtract(_liquidity_pool_entry_body.2, 'Tuple(
                    params Tuple(
                        fee UInt64,
                        asset_a String,
                        asset_b String
                    ),
                    reserve_a Int64,
                    reserve_b Int64,
                    total_pool_shares Int64,
                    pool_shares_trust_line_count Int64
                )') _liquidity_pool_constant_product,

                JSONExtractKeysAndValues(_liquidity_pool_constant_product.params.asset_a, 'String')[1] as _asset_a_type_and_data,
                JSONExtractKeysAndValues(_liquidity_pool_constant_product.params.asset_b, 'String')[1] as _asset_b_type_and_data,

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

                _liquidity_pool_entry.liquidity_pool_id as liquidity_pool_id,
                _liquidity_pool_entry_body.1 as type,
                _liquidity_pool_constant_product.params.fee as fee,
                _liquidity_pool_constant_product.total_pool_shares as total_pool_shares,
                _liquidity_pool_constant_product.pool_shares_trust_line_count as pool_shares_trust_line_count,
                _liquidity_pool_constant_product.reserve_a as reserve_a,
                _liquidity_pool_constant_product.reserve_b as reserve_b,

                if(
                    JSONType(_liquidity_pool_constant_product.params.asset_a) = 'Object',
                    _asset_a_type_and_data.1,
                    _liquidity_pool_constant_product.params.asset_a
                ) as asset_a_type,

                JSONExtractString(_asset_a_type_and_data.2, 'asset_code') as asset_a_code,
                JSONExtractString(_asset_a_type_and_data.2, 'issuer') as asset_a_issuer,
                stellar_asset_id(asset_a_code, asset_a_issuer, asset_a_type) as asset_a_id,

                if(
                    JSONType(_liquidity_pool_constant_product.params.asset_b) = 'Object',
                    _asset_b_type_and_data.1,
                    _liquidity_pool_constant_product.params.asset_b
                ) as asset_b_type,

                JSONExtractString(_asset_b_type_and_data.2, 'asset_code') as asset_b_code,
                JSONExtractString(_asset_b_type_and_data.2, 'issuer') as asset_b_issuer,
                stellar_asset_id(asset_b_code, asset_b_issuer, asset_b_type) as asset_b_id
            from iceberg('{{ .ICEBERG_CHANGES_URL_CLICKHOUSE }}', NOSIGN, settings iceberg_use_version_hint=1) as changes
            where ledger_sequence >= {{.RANGE_START}}
            AND ledger_sequence <= {{.RANGE_END}}
            and ledger_entry_type = 'liquidity_pool'
        )

        select 
            columns('^[^_]')
        from liquidity_pools
)

{{end}}


https://stellar-iceberg-testnet.agnostic.tech/changes'