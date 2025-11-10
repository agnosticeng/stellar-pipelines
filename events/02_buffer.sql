{{define "create_buffer"}}

create table buffer as range_{{.RANGE_START}}_{{.RANGE_END}}
engine = MergeTree 
order by {{.ORDER_BY}}
settings old_parts_lifetime=10

{{end}}

{{define "insert_into_buffer"}}

insert into table buffer
select * from range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}}

{{end}}

{{define "drop_range"}}

drop table range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}} sync

{{end}}

{{define "merge_vars"}}

select
    least(
        {{.LEFT.RANGE_START | default "18446744073709551615"}}, 
        {{.RIGHT.RANGE_START}}
    ) as RANGE_START,
    greatest(
        {{.LEFT.RANGE_END | default "0"}}, 
        {{.RIGHT.RANGE_END}}
    ) as RANGE_END,
    generateUUIDv7() || '.parquet' as OUTPUT_FILE

{{end}}

{{define "condition"}}

select toUInt64(count(*)) >= {{.MAX_BUFFER_SIZE | default "1000"}} as value from buffer

{{end}}

{{define "rename_buffer"}}

rename buffer to buffer_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}