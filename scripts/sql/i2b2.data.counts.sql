-- MUSC aggregates for 
create table
obs_fact_MUSC_counts
nologging
as
(
select concept_cd, count(1) total_recs,
count(patient_num) pat_counts,
count(distinct encounter_num) enc_counts
from i2b2MUSCdata.observation_fact
group by concept_cd
);



