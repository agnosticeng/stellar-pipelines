{{define "merge_range_and_buffer_vars"}}

select
    least(
        {{.LEFT.RANGE_START | default "18446744073709551615"}}, 
        {{.RIGHT.RANGE_START}}
    ) as RANGE_START,
    greatest(
        {{.LEFT.RANGE_END | default "0"}}, 
        {{.RIGHT.RANGE_END}}
    ) as RANGE_END
    
{{end}}