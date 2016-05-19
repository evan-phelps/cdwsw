select
  sex,
  race,
  ethnicity,
  age,
  marital_status,
  to_char(birth_date, 'YYYY-MM') bdate,
  to_char(death_date, 'YYYY-MM') ddate,
  deceased_ind,
  urban_rural,
  military_status,
  count(1),
  min(patient_id),
  max(patient_id),
  grouping(sex) m_sex,
  grouping(race) m_race,
  grouping(ethnicity) m_ethnicity,
  grouping(age) m_age,
  grouping(marital_status) m_marital_status,
  grouping(to_char(birth_date, 'YYYY-MM')) m_bdate,
  grouping(to_char(death_date, 'YYYY-MM')) m_ddate,
  grouping(deceased_ind) m_deceased_ind,
  grouping(urban_rural) m_urban_rural,
  grouping(military_status) m_military_status
from cdw.patient pat
where exists
  (
    select 1 from cdw.visit enc
    where enc.patient_id = pat.patient_id)
group by
  grouping sets( sex,
                 race,
                 ethnicity,
                 age,
                 marital_status,
                 to_char(birth_date, 'YYYY-MM'),
                 to_char(death_date, 'YYYY-MM'),
                 deceased_ind,
                 urban_rural,
                 military_status)
;
