{{define "append_to_table"}}

select 
    result
from executable(
    'ch-iceberg table-function iceberg-append-static-table --table-location={{ .ICEBERG_URL }}',
    ArrowStream, 
    'result String',
    (
        select * from buffer_{{.RANGE_START}}_{{.RANGE_END}}
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_read_timeout=100000
)

{{end}}

{{define "drop_buffer"}}

drop table buffer_{{.RANGE_START}}_{{.RANGE_END}} sync

{{end}}