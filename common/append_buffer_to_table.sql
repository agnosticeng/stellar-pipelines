{{define "append_buffer_to_table"}}

select 
    result
from executable(
    'ch-iceberg table-function iceberg-append {{ .ICEBERG_CATALOG_PROPERTIES }} {{ .ICEBERG_SINK_TABLE }}',
    ArrowStream, 
    'result String',
    (
        select * from buffer_{{.RANGE_START}}_{{.RANGE_END}}
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_termination_timeout=120,
        command_read_timeout=120000,
        command_write_timeout=120000
)

{{end}}