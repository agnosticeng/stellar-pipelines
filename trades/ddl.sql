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
                        {"id": 4    , "name": "transaction_hash"                            , "type": "binary"      , "required": true},
                        {"id": 5    , "name": "transaction_id"                              , "type": "long"        , "required": true},                     
                        {"id": 6    , "name": "operation_id"                                , "type": "binary"      , "required": true},
                        {"id": 7    , "name": "operation_type"                              , "type": "binary"      , "required": true},
                        {"id": 8    , "name": "trade_type"                                  , "type": "binary"      , "required": true},
                        {"id": 9    , "name": "order"                                       , "type": "int"         , "required": true},
                        {"id": 10   , "name": "selling_asset_type"                          , "type": "binary"      , "required": true},
                        {"id": 11   , "name": "selling_asset_code"                          , "type": "binary"      , "required": true},
                        {"id": 12   , "name": "selling_asset_issuer"                        , "type": "binary"      , "required": true},
                        {"id": 13   , "name": "selling_asset_id"                            , "type": "binary"      , "required": true},
                        {"id": 14   , "name": "selling_amount"                              , "type": "long"        , "required": true},
                        {"id": 15   , "name": "buying_asset_type"                           , "type": "binary"      , "required": true},
                        {"id": 16   , "name": "buying_asset_code"                           , "type": "binary"      , "required": true},
                        {"id": 17   , "name": "buying_asset_issuer"                         , "type": "binary"      , "required": true},
                        {"id": 18   , "name": "buying_asset_id"                             , "type": "binary"      , "required": true},
                        {"id": 19   , "name": "buying_amount"                               , "type": "long"        , "required": true},
                        {"id": 20   , "name": "seller_id"                                   , "type": "binary"      , "required": true},
                        {"id": 21   , "name": "selling_offer_id"                            , "type": "binary"      , "required": true},
                        {"id": 22   , "name": "liquidity_pool_id"                           , "type": "binary"      , "required": true},
                        {"id": 23   , "name": "liquidity_pool_fee"                          , "type": "long"        , "required": true},
                        {"id": 24   , "name": "buying_offer_id"                             , "type": "binary"      , "required": true}
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
                        {"source-id": 13 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id": 18 , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id": 8  , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id": 6  , "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                        {"source-id": 9  , "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
                    ]
                }
            }
            $JSON$
        )
    ) as res

select
    throwIf(empty(res.error::String) = 0 and startsWithCaseInsensitive(res.error::String, 'Table already exists') = 0) as _1

{{end}}