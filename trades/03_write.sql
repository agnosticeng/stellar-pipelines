{{define "write_parquet_file"}}

insert into function s3('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}/data/{{.OUTPUT_FILE}}')
select * from buffer_{{.RANGE_START}}_{{.RANGE_END}}
order by {{.ORDER_BY}}

{{end}}

{{define "iceberg_commit"}}

select icepq_add(concat('s3:/', path('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}')), ['{{.OUTPUT_FILE}}'])

{{end}}

{{define "drop_buffer"}}

drop table buffer_{{.RANGE_START}}_{{.RANGE_END}} sync

{{end}}