{{define "insert_range_into_buffer"}}

insert into table buffer
select * from range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}}

{{end}}