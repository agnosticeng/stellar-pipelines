{{define "init_start"}}

select 
    arrayMax(res.value[].upper)::UInt64 as INIT_START, 
    throwIf(res.error::String = 'table does not exist', 'table does not exists', 8888::Int16) as _1,
    throwIf(res.error::String <> '', res.error::String) as _2
from (select icepq_field_bound_values(concat('s3:/', path('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}')), 'sequence') as res) 
settings allow_custom_error_code_in_throwif=true

{{end}}