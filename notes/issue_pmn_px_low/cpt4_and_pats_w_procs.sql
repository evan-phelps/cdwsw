--(12:11:52 PM) Rick Larson: 415 459-5234

select
  count(1),
  count(patient_id),
  count(distinct patient_id)
from
  cdw.visit enc
where exists ( select 1 from cdw.visit_deid_map_cc encmap
               where encmap.visit_id = enc.visit_id )
  and (   exists ( select 1 from CDW.PROCEDURE_CPT4_TEMP t1
                   where t1.visit_id = enc.visit_id )
       or exists ( select 1 from CDW.PROCEDURE t2
               where t2.visit_id = enc.visit_id )
      )
;

select
  enc.visit_id,
  enc.patient_id,
  enc.datasource_id,
  case when exists ( select 1 from CDW.PROCEDURE_CPT4_TEMP t1
                     where t1.visit_id = enc.visit_id )
       then 'Y' else 'N'
  end AS new_proc,
  case when exists ( select 1 from CDW.PROCEDURE t1
                     where t1.visit_id = enc.visit_id
                       and (    enc.datasource_id != 1
                             or t1.proc_code_type != 'CPT4'
                           )
                   )
       then 'Y' else 'N'
  end AS cdw_proc
from cdw.visit enc
inner join cdw.visit_deid_map_cc encmap
  on ( enc.visit_id = encmap.visit_id )
;

----------------

select
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where exists (select 1 from cdw.procedure px
              where px.visit_id = enc.visit_id)
   or exists (select 1 from cdw.procedure_cpt4_temp px
              where px.visit_id = enc.visit_id)
;
--2016843

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
;
--2880075

select /*+ parallel 4 */
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        ) inst,
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where exists (select 1 from cdw.procedure px
              where px.visit_id = enc.visit_id)
   or exists (select 1 from cdw.procedure_cpt4_temp px
              where px.visit_id = enc.visit_id)
group by 
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        )
order by inst
;
--GHS	930700
--MUSC	695468
--PH	432055
--SRHS	61392

select /*+ parallel 4 */
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        ) inst,
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
group by 
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        )
order by inst
;
--GHS	1042394
--MUSC	850083
--PH	956747
--SRHS	350005

select /*+ parallel 4 */
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        ) inst,
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where exists (select 1 from cdw.procedure px
              where px.visit_id = enc.visit_id
                and not (    px.proc_code_type = 'CPT4'
                         and px.datasource_id = 2 )
             )
group by
  decode(enc.datasource_id, 1, 'MUSC',
                           25, 'MUSC',
                            2, 'GHS',
                            3, 'PH',
                           14, 'SRHS',
                           'UNK'
        )
order by inst
;
--GHS	384379
--MUSC	366382
--PH	432055
--SRHS	61392

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where exists (select 1 from cdw.procedure px
              where px.visit_id = enc.visit_id)
;
--1709571

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where exists (select 1 from cdw.procedure_cpt4_temp px
              where px.visit_id = enc.visit_id
                and px.datasource_id != 2)
   or exists (select 1 from cdw.procedure px
              where px.visit_id = enc.visit_id
                and not (    px.datasource_id = 2
                         and px.proc_code_type = 'CPT4' )
             )
;
--1513459

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where enc.datasource_id != 2
  and (   exists (select 1 from cdw.procedure_cpt4_temp px
                  where px.visit_id = enc.visit_id
                    and px.datasource_id != 2)
       or exists (select 1 from cdw.procedure px
                  where px.visit_id = enc.visit_id
                    and px.datasource_id != 2
             )
      )
;
--1163654

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join ephelps.visit_ids_cc emap
  on (enc.visit_id = emap.visit_id)
where enc.datasource_id != 2
;
--2087064

---------------
--DOUBLE-CHECK
select count(distinct patient_id)
from cdw.patient_deid_map_cc
;
--2879835

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join cdw.visit_deid_map_cc encmap
  on ( encmap.visit_id = enc.visit_id )
;
--2880075

select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join cdw.visit_deid_map_cc encmap
  on ( encmap.visit_id = enc.visit_id )
where exists ( select 1 from cdw.procedure px
               where px.visit_id = enc.visit_id )
   or exists ( select 1 from cdw.procedure_cpt4_temp px
               where px.visit_id = enc.visit_id )
;
--2016843


select /*+ parallel 4 */
  count(distinct patient_id)
from cdw.visit enc
inner join cdw.visit_deid_map_cc encmap
  on ( encmap.visit_id = enc.visit_id )
where /* exists ( select 1 from cdw.procedure px
               where px.visit_id = enc.visit_id )
   or */ exists ( select 1 from cdw.procedure_cpt4_temp px
               where px.visit_id = enc.visit_id )
;
--2016843 (either)
--1709571  (procedure table only; cGHS not cMUSC)
--1515965  (procedure_cpt4_temp only; cGHS and cMUSC)
