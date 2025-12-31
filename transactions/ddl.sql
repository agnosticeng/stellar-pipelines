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
                        {"id": 4    , "name": "hash"                                        , "type": "binary"      , "required": true},
                        {"id": 5    , "name": "id"                                          , "type": "long"        , "required": true},
                        {"id": 6    , "name": "inner_transaction_hash"                      , "type": "binary"      , "required": false},
                        {"id": 7    , "name": "account"                                     , "type": "binary"      , "required": true},
                        {"id": 8    , "name": "account_muxed"                               , "type": "binary"      , "required": false},
                        {"id": 9    , "name": "account_sequence"                            , "type": "long"        , "required": false},
                        {"id": 10   , "name": "fee_account"                                 , "type": "string"      , "required": false},
                        {"id": 11   , "name": "fee_account_muxed"                           , "type": "string"      , "required": false},
                        {"id": 12   , "name": "operation_count"                             , "type": "int"         , "required": false},
                        {"id": 13   , "name": "result_code"                                 , "type": "string"      , "required": false},
                        {"id": 14   , "name": "successful"                                  , "type": "int"         , "required": false},
                        {"id": 15   , "name": "memo_type"                                   , "type": "string"      , "required": false},
                        {"id": 16   , "name": "memo"                                        , "type": "string"      , "required": false},
                        {"id": 17   , "name": "min_time_bound"                              , "type": "timestamp"   , "required": false},
                        {"id": 18   , "name": "max_time_bound"                              , "type": "timestamp"   , "required": false},
                        {"id": 19   , "name": "min_ledger_bound"                            , "type": "int"         , "required": false},
                        {"id": 20   , "name": "max_ledger_bound"                            , "type": "int"         , "required": false},
                        {"id": 21   , "name": "min_account_sequence"                        , "type": "long"        , "required": false},
                        {"id": 22   , "name": "min_account_sequence_age"                    , "type": "long"        , "required": false},
                        {"id": 23   , "name": "min_account_sequence_ledger_gap"             , "type": "int"         , "required": false},
                        {"id": 24   , "name": "extra_signers"                               , "type": {"type": "list", "element-id": 25, "element-required": true, "element": "string"}, "required": false},
                        {"id": 26   , "name": "max_fee"                                     , "type": "long"        , "required": false},
                        {"id": 27   , "name": "fee_charged"                                 , "type": "long"        , "required": false},
                        {"id": 28   , "name": "new_max_fee"                                 , "type": "long"        , "required": false},
                        {"id": 29   , "name": "resource_fee"                                , "type": "long"        , "required": false},
                        {"id": 30   , "name": "total_non_refundable_resource_fee_charged"   , "type": "long"        , "required": false},
                        {"id": 31   , "name": "total_refundable_resource_fee_charged"       , "type": "long"        , "required": false},
                        {"id": 32   , "name": "rent_fee_charged"                            , "type": "long"        , "required": false},
                        {"id": 33   , "name": "resource_fee_charged"                        , "type": "long"        , "required": false},
                        {"id": 34   , "name": "inclusion_fee"                               , "type": "long"        , "required": false},
                        {"id": 35   , "name": "inclusion_fee_charged"                       , "type": "long"        , "required": false},
                        {"id": 36   , "name": "soroban_resources_instructions"              , "type": "int"         , "required": false},
                        {"id": 37   , "name": "soroban_resources_read_bytes"                , "type": "long"        , "required": false},
                        {"id": 38   , "name": "soroban_resources_write_bytes"               , "type": "long"        , "required": false}
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
                        {"source-id": 7, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
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