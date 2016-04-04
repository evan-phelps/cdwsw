
--HSSC Aggregates for observation facts
create table
obs_fact_HSSC_counts
nologging
as
(
select concept_cd, count(1) total_recs,
count(patient_num) pat_counts,
count(distinct encounter_num) enc_counts
from i2b2HSSCdata.observation_fact
group by concept_cd
);

-- MUSC aggregates for observation facts 
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

-- GHS Aggregates for observation facts
create table
obs_fact_GHS_counts
nologging
as
(
select concept_cd, count(1) total_recs,
count(patient_num) pat_counts,
count(distinct encounter_num) enc_counts
from i2b2GHSdata.observation_fact
group by concept_cd
);


-- PH Aggregates for observation facts
create table
obs_fact_PH_counts
nologging
as
(
select concept_cd, count(1) total_recs,
count(patient_num) pat_counts,
count(distinct encounter_num) enc_counts
from i2b2PHdata.observation_fact
group by concept_cd
);


--SRHS Aggregates for observation facts
create table
obs_fact_SRHS_counts
nologging
as
(
select concept_cd, count(1) total_recs,
count(patient_num) pat_counts,
count(distinct encounter_num) enc_counts
from i2b2SRHSdata.observation_fact
group by concept_cd
);




/* Once the aggregated tables are created join on the concept dimension to separate 
various types of observations (diags,procs,meds,labs etc)*/

--Join aggregated table obs_fact_HSSC_count with concept_dimension table
create table /* + parallel 4 */
obs_HSSC_counts_with_dtype
nologging
as
(select ob.CONCEPT_CD temp_concept_cd,
cdim.concept_cd cdim_concept_cd,
ob.total_recs total_obs,
ob.pat_counts pat_counts,
ob.enc_counts enc_counts,
cdim.dtype dtype_from_concept_dim,
cdim.npaths
from obs_fact_HSSC_counts ob
full outer join
(
select concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2) dtype,
count(1) npaths
from i2b2HSSCdata.concept_dimension dim
group by concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2)
) cdim
on(cdim.concept_cd = ob.concept_cd)
);


--Join aggregated table obs_fact_MUSC_count with concept_dimension table
create table /* + parallel 4 */
obs_MUSC_counts_with_dtype
nologging
as
(select ob.CONCEPT_CD temp_concept_cd,
cdim.concept_cd cdim_concept_cd,
ob.total_recs total_obs,
ob.pat_counts pat_counts,
ob.enc_counts enc_counts,
cdim.dtype dtype_from_concept_dim,
cdim.npaths
from obs_fact_MUSC_counts ob
full outer join
(
select concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2) dtype,
count(1) npaths
from i2b2MUSCdata.concept_dimension dim
group by concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2)
) cdim
on(cdim.concept_cd = ob.concept_cd)
);



--Join aggregated table obs_fact_GHS_count with concept_dimension table
create table /* + parallel 4 */
obs_GHS_counts_with_dtype
nologging
as
(select ob.CONCEPT_CD temp_concept_cd,
cdim.concept_cd cdim_concept_cd,
ob.total_recs total_obs,
ob.pat_counts pat_counts,
ob.enc_counts enc_counts,
cdim.dtype dtype_from_concept_dim,
cdim.npaths
from obs_fact_GHS_counts ob
full outer join
(
select concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2) dtype,
count(1) npaths
from i2b2GHSdata.concept_dimension dim
group by concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2)
) cdim
on(cdim.concept_cd = ob.concept_cd)
);



--Join aggregated table obs_fact_PH_count with concept_dimension table
create table /* + parallel 4 */
obs_PH_counts_with_dtype
nologging
as
(select ob.CONCEPT_CD temp_concept_cd,
cdim.concept_cd cdim_concept_cd,
ob.total_recs total_obs,
ob.pat_counts pat_counts,
ob.enc_counts enc_counts,
cdim.dtype dtype_from_concept_dim,
cdim.npaths
from obs_fact_PH_counts ob
full outer join
(
select concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2) dtype,
count(1) npaths
from i2b2PHdata.concept_dimension dim
group by concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2)
) cdim
on(cdim.concept_cd = ob.concept_cd)
);



--Join aggregated table obs_fact_SRHS_count with concept_dimension table
create table /* + parallel 4 */
obs_SRHS_counts_with_dtype
nologging
as
(select ob.CONCEPT_CD temp_concept_cd,
cdim.concept_cd cdim_concept_cd,
ob.total_recs total_obs,
ob.pat_counts pat_counts,
ob.enc_counts enc_counts,
cdim.dtype dtype_from_concept_dim,
cdim.npaths
from obs_fact_SRHS_counts ob
full outer join
(
select concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2) dtype,
count(1) npaths
from i2b2SRHSdata.concept_dimension dim
group by concept_cd,substr(concept_path,2,instr(concept_path,'\',2)-2)
) cdim
on(cdim.concept_cd = ob.concept_cd)
);

