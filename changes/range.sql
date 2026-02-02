{{define "create_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with
        {{template "changes" .}}

    select * from changes
)

{{end}}
