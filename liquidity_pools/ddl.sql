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
                        {"id": 1    , "name": "ledger_sequence"                             , "type": "int"         , "required": true},
                        {"id": 2    , "name": "ledger_close_time"                           , "type": "timestamp"   , "required": true},
                        {"id": 3    , "name": "ledger_hash"                                 , "type": "binary"      , "required": true},
                        {"id": 4    , "name": "transaction_hash"                            , "type": "binary"      , "required": false},
                        {"id": 5    , "name": "transaction_id"                              , "type": "long"        , "required": false},
                        {"id": 6    , "name": "transaction_result_code"                     , "type": "string"      , "required": false},
                        {"id": 7    , "name": "transaction_successful"                      , "type": "int"         , "required": false},
                        {"id": 8    , "name": "operation_id"                                , "type": "long"        , "required": false},
                        {"id": 9    , "name": "operation_result_code"                       , "type": "string"      , "required": false},
                        {"id": 10   , "name": "operation_inner_result_code"                 , "type": "string"      , "required": false},
                        {"id": 11   , "name": "change_source"                               , "type": "string"      , "required": true},
                        {"id": 12   , "name": "change_type"                                 , "type": "string"      , "required": true},
                        {"id": 13   , "name": "change_last_modified_ledger_sequence"        , "type": "long"        , "required": true},
                        {"id": 14   , "name": "liquidity_pool_id"                           , "type": "string"      , "required": true},
                        {"id": 15   , "name": "type"                                        , "type": "string"      , "required": true},
                        {"id": 16   , "name": "fee"                                         , "type": "long"        , "required": true},
                        {"id": 17   , "name": "total_pool_shares"                           , "type": "long"        , "required": true},
                        {"id": 18   , "name": "pool_shares_trust_line_count"                , "type": "long"        , "required": true},
                        {"id": 19   , "name": "reserve_a"                                   , "type": "long"        , "required": true},
                        {"id": 20   , "name": "reserve_b"                                   , "type": "long"        , "required": true},
                        {"id": 21   , "name": "asset_a_type"                                , "type": "string"      , "required": true},
                        {"id": 22   , "name": "asset_a_code"                                , "type": "string"      , "required": true},
                        {"id": 23   , "name": "asset_a_issuer"                              , "type": "string"      , "required": true},
                        {"id": 24   , "name": "asset_a_id"                                  , "type": "binary"      , "required": true},
                        {"id": 25   , "name": "asset_b_type"                                , "type": "string"      , "required": true},
                        {"id": 26   , "name": "asset_b_code"                                , "type": "string"      , "required": true},
                        {"id": 27   , "name": "asset_b_issuer"                              , "type": "string"      , "required": true},
                        {"id": 28   , "name": "asset_b_id"                                  , "type": "binary"      , "required": true}
                    ]
                },

                "partition-spec": {
                    "spec-id": 0,
                    "fields": [
                        {"source-id": 2, "field-id": 1000, "name": "ledger_closed_time_year", "transform": "year"}
                    ]
                },
                "write-order": {
                    "order-id": 0,
                    "fields": [
                        {"source-id": 14 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id":  1 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id":  5 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id":  8 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
                    ]
                }
            }
            $JSON$
        )
    ) as res

select
    throwIf(empty(res.error::String) = 0 and startsWithCaseInsensitive(res.error::String, 'Table already exists') = 0) as _1

{{end}}