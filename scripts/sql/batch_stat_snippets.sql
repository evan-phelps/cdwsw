select
  job_name,
  state,
  failure_count,
  next_run_date,
  last_start_date,
  last_run_duration
from USER_SCHEDULER_JOBS
order by job_name
;

select *
from SYS.USER_SCHEDULER_JOB_RUN_DETAILS
;

select *
from HSSC_ETL.CDW_INCR_MPI_CNTRL
;

select
  batch_id,
  time_start,
  round((time_last-time_start)*24*60, 2) duration_m,
  n_merged,
  round(n_merged/((time_last-time_start)*24*60)) rate_m
from (
  select batch_id, count(1) n_merged
  from hssc_etl.cdw_incr_mpi_stg
  group by batch_id
)
inner join hssc_etl.cdw_incr_mpi_cntrl
using (batch_id)
order by batch_id
;

select
  to_char(time, 'YYYY-MM-DD HH24:MI:SS') ts,
  concat('|', log_msg)
from hssc_incr_log
order by ts desc
;

