select
  concept_cd,
  tval_char,
  valtype_cd,
  count(1),
  count(encounter_num),
  count(distinct encounter_num),
  count(patient_num),
  count(distinct patient_num),
  count(start_date),
  count(distinct start_date),
  count(nval_num),
  count(distinct nval_num),
  count(units_cd),
  count(distinct units_cd),
  min(nval_num),
  avg(nval_num),
  max(nval_num)
from i2b2HSSCdata.observation_fact
where concept_cd like 'LOINC%'
group by
  concept_cd,
  tval_char,
  valtype_cd
order by count(1) desc
;

