{{define "create_buffer_from_range"}}

create table buffer as range_{{.RANGE_START}}_{{.RANGE_END}}
engine = MergeTree 
order by ({{.ICEBERG_SINK_TABLE_ORDER_BY }})
settings 
    old_parts_lifetime=10,
    allow_nullable_key=1

{{ end }}