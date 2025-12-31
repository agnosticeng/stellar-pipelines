{{define "source"}}

with 
    {{ .MAX_BATCH_SIZE | default "128" }} as max_batch_size,
    {{ .MAX_BATCH_PER_RUN | default "100000" }} as max_batch_per_run,

    (
        select iceberg_field_bound_values_static_table(
            '{{ .ICEBERG_CHANGES_URL }}',
            'ledger_sequence'
        )
    ) as res,

    (select arrayMax(res.value[].upper::Array(Int64))) AS end,

    firstNonDefault(
        {{.RANGE_END | toCH}} + 1,
        {{.DEFAULT_START | default "0" | toCH}}::UInt32,
        1
    ) as start

select 
    greatest(start, generate_series) as RANGE_START,
    least(end, (generate_series + max_batch_size - 1)) as RANGE_END
from generate_series(
    (intDiv(start, max_batch_size) * max_batch_size)::UInt32,
    (intDiv(end, max_batch_size) * max_batch_size)::UInt32,
    max_batch_size::UInt32
)
limit max_batch_per_run

{{end}}
