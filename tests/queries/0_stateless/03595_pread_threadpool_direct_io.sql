-- Tags: stateful

drop table if exists 03595_data;

create table 03595_data (key UInt32, val String) engine = MergeTree order by key
as
select number, 'val-' || number from numbers(100000);

select * from 03595_data
format Null
settings
    local_filesystem_read_method = 'pread_threadpool',
    min_bytes_to_use_direct_io = 1,
    log_query_threads = 1;

select * from 03595_data
format Null
settings
    local_filesystem_read_method = 'pread_threadpool',
    min_bytes_to_use_direct_io = 1,
    log_query_threads = 1;

system flush logs query_log, query_thread_log;

with queries as (
    select query_id, row_number() over(order by event_time_microseconds) as ordinal
    from system.query_log
    where type = 'QueryFinish'
     and current_database = currentDatabase()
     and query like 'select * from 03595_data%'
)
select queries.ordinal, thread_name, sum(ProfileEvents['OSReadBytes']) > 0 as bytes_read
from system.query_thread_log qtl
    join queries
        on queries.query_id = qtl.query_id
where current_database = currentDatabase()
  and query_id in (select query_id from queries)
  and ProfileEvents['OSReadBytes'] > 0
group by 1, 2
order by 1, 2;

drop table 03595_data;
