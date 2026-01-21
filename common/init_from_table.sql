{{define "init_from_table"}}

with 
   (
        select iceberg_field_bound_values(
            '{{ .ICEBERG_CATALOG_PROPERTIES }}', 
            '{{ .ICEBERG_SINK_TABLE }}',
            '{{ .ICEBERG_SINK_TABLE_MARKER_FIELD }}'
        )
    ) as res

select arrayMax(res.value[].upper::Array(UInt64)) AS RANGE_END

{{end}}