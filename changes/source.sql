{{define "source"}}

with 
    {{ .LEDGERS_PER_FILE | default "64" }} as ledgers_per_file,
    {{ .FILES_PER_BATCH | default "1" }} as files_per_batch,
    {{ .MAX_BATCH_PER_RUN | default "100000" }} as max_batch_per_run,
    {{ .TIP_DISTANCE | default "128" }} as tip_distance,
    '{{ .RPC_URL | default "https://rpc.lightsail.network" }}' as rpc_endpoint,
    ledgers_per_file * files_per_batch as max_batch_size,

    (
        select stellar_rpc(rpc_endpoint || '/#fail-on-error=true&fail-on-null=true', 'getHealth', 'null')
    ) as health,

    (health.value.latestLedger::UInt32 - tip_distance::UInt32) as end,

    firstNonDefault(
        {{.RANGE_END | toCH}} + 1,
        {{.DEFAULT_START | toCH}}::UInt32,
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
