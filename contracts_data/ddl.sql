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
                        {"id": 14   , "name": "contract"                                    , "type": "string"      , "required": true},
                        {"id": 15   , "name": "durability"                                  , "type": "string"      , "required": true},
                        {"id": 16   , "name": "key"                                         , "type": "string"      , "required": true},
                        {"id": 17   , "name": "value"                                       , "type": "string"      , "required": true}

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