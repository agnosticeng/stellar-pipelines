{{define "source"}}

with 
    {{.MAX_BATCH_SIZE | default "10"}} as max_batch_size,
    {{.MAX_BATCH_PER_RUN | default "100"}} as max_batch_per_run,
    {{.RPC_ENDPOINT | default "'https://rpc.lightsail.network'"}} as rpc_endpoint,

    (
        select stellar_rpc(rpc_endpoint || '/#fail-on-error=true&fail-on-null=true', 'getHealth', 'null')
    ) as health,

    (health.value.latestLedger::UInt32 - 64) as end,

    coalesce(
        {{.RANGE_END | toCH}} + 1,
        {{.INIT_START | toCH}},
        {{.DEFAULT_START | toCH}},
        1
    ) as start

select 
    generate_series as RANGE_START,
    least(end, (generate_series + max_batch_size - 1)) as RANGE_END
from generate_series(
    start::UInt32,
    end::UInt32,
    max_batch_size::UInt32
)
limit max_batch_per_run

{{end}}