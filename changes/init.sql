{{define "init_start"}}

with 
    (
        select iceberg_field_bound_values_static_table(
            '{{ .ICEBERG_URL }}',
            'ledger_sequence'
        )
    ) as res

select arrayMax(res.value[].upper::Array(UInt64)) AS RANGE_END

{{end}}