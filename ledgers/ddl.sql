{{define "create_table"}}

with 
    (
        select iceberg_create_static_table(
            '{{ .ICEBERG_URL }}',
            $JSON$
            {
                "name": "",
                "schema": {
                    "schema-id": 0,
                    "type": "struct",
                    "fields": [
                        {"id": 1    , "name": "sequence"                                  , "type": "int"         , "required": true},
                        {"id": 2    , "name": "close_time"                                , "type": "timestamp"   , "required": true},
                        {"id": 3    , "name": "hash"                                      , "type": "binary"      , "required": true},
                        {"id": 4    , "name": "id"                                        , "type": "long"        , "required": true},
                        {"id": 5    , "name": "previous_ledger_hash"                      , "type": "binary"      , "required": false},
                        {"id": 6    , "name": "total_coins"                               , "type": "long"        , "required": false},
                        {"id": 7    , "name": "fee_pool"                                  , "type": "long"        , "required": false},
                        {"id": 8    , "name": "base_fee"                                  , "type": "long"        , "required": false},
                        {"id": 9    , "name": "base_reserve"                              , "type": "long"        , "required": false},
                        {"id": 10   , "name": "max_tx_set_size"                           , "type": "int"         , "required": false},
                        {"id": 11   , "name": "ledger_version"                            , "type": "int"         , "required": false},
                        {"id": 12   , "name": "transaction_count"                         , "type": "int"         , "required": false},
                        {"id": 13   , "name": "successful_transaction_count"              , "type": "int"         , "required": false},
                        {"id": 14   , "name": "failed_transaction_count"                  , "type": "int"         , "required": false},
                        {"id": 15   , "name": "node_id"                                   , "type": "binary"      , "required": false},
                        {"id": 16   , "name": "signature"                                 , "type": "binary"      , "required": false},
                        {"id": 17   , "name": "total_byte_size_of_live_soroban_state"     , "type": "long"        , "required": false},
                        {"id": 18   , "name": "soroban_fee_write1_kb"                     , "type": "long"        , "required": false},
                        {"id": 19   , "name": "operation_count"                           , "type": "int"         , "required": false},
                        {"id": 20   , "name": "tx_set_operation_count"                    , "type": "int"         , "required": false}
                    ]
                },
                "partition-spec": {
                    "spec-id": 0,
                    "fields": [
                        {"source-id": 2, "field-id": 1000, "name": "close_time_year", "transform": "year"}
                    ]
                },
                "write-order": {
                    "order-id": 0,
                    "fields": [
                        {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
                    ]
                }
            }
            $JSON$
        )
    ) as res

select
    throwIf(empty(res.error::String) = 0 and startsWithCaseInsensitive(res.error::String, 'Table already exists') = 0) as _1

{{end}}