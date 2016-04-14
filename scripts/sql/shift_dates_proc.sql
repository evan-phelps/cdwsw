--------------------------------------------------------
--  File created - Thursday-April-14-2016   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SHIFT_DATES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDW"."SHIFT_DATES" 
IS
BEGIN
--Patient table date shifts
update cdw.patient_deid_map_hssc set shiftvalue = dbms_random.value(1, 365);
update cdw.patient_deid_map_hssc m set shifted_birth_date = ( select
P.BIRTH_DATE - m.shiftvalue
from cdw.patient p where p.patient_id = m.patient_id);

update cdw.patient_deid_map_hssc m set shifted_death_date = ( select
P.death_date - m.shiftvalue
from cdw.patient p where p.patient_id = m.patient_id)
where m.patient_id in (select p.patient_id from cdw.patient p where death_date is not null);

update cdw.patient_deid_map_hssc set shifted_age = months_between(coalesce(shifted_death_date,sysdate),shifted_birth_date)/12;

update cdw.patient_deid_map_hssc set shifted_age = 85 where shifted_age > 85;

update cdw.patient_deid_map_hssc set shifted_age = 0 where shifted_age < 0;

--Visit table date shifts
update cdw.visit_deid_map_hssc v set shiftvalue = (select shiftvalue from cdw.patient_deid_map_hssc p where p.patient_deid = v.patient_deid);

update cdw.visit_deid_map_hssc m set shifted_start_date = (select v.visit_start_date - m.shiftvalue from
cdw.visit v where v.visit_id = m.visit_id),
shifted_end_date = (select v.visit_end_date - m.shiftvalue from 
cdw.visit v where v.visit_id = m.visit_id);

update cdw.visit_deid_map_hssc m set shifted_age_at_visit = ( select months_between(m.shifted_start_date,p.SHIFTED_BIRTH_DATE)/12
from cdw.patient_deid_map_hssc p where p.patient_deid = m.patient_deid);

commit;
END Shift_Dates;

/
