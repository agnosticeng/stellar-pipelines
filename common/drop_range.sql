{{define "drop_range"}}

drop table range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}} sync

{{end}}