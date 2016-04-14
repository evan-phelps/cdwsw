select
'Age' attr,
a.AGE_IN_YEARS_NUM value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Age:%'
) ov
full outer join (
select
AGE_IN_YEARS_NUM,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by AGE_IN_YEARS_NUM
) a on (ov.c_dimcode = to_char(a.AGE_IN_YEARS_NUM)) 
;            

select
'Ethnicity' attr,
a.ETHNICITY_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Ethnicity:%'
) ov
full outer join (
select
ETHNICITY_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by ETHNICITY_CD
) a on (ov.c_dimcode = to_char(a.ETHNICITY_CD)) 
;            

select
'Urban-Rural Classification' attr,
a.URBAN_RURAL_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Urban-Rural Classification:%'
) ov
full outer join (
select
URBAN_RURAL_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by URBAN_RURAL_CD
) a on (ov.c_dimcode = to_char(a.URBAN_RURAL_CD)) 
;            

select
'Vital Status' attr,
a.VITAL_STATUS_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Vital Status:%'
) ov
full outer join (
select
VITAL_STATUS_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by VITAL_STATUS_CD
) a on (ov.c_dimcode = to_char(a.VITAL_STATUS_CD)) 
;            

select
'Military Status' attr,
a.MILITARY_STATUS_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Military Status:%'
) ov
full outer join (
select
MILITARY_STATUS_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by MILITARY_STATUS_CD
) a on (ov.c_dimcode = to_char(a.MILITARY_STATUS_CD)) 
            

select
'Sex' attr,
a.SEX_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Sex:%'
) ov
full outer join (
select
SEX_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by SEX_CD
) a on (ov.c_dimcode = to_char(a.SEX_CD)) 
            

select
'Race' attr,
a.RACE_CD value,
ov.c_name ,
ov.c_fullname,
a.n_recs pat_count
from ( select *
from I2B2HSSCMETA.ONT_DEMO 
where c_basecode like 'Race:%'
) ov
full outer join (
select
RACE_CD,
count(1) n_recs
from I2B2HSSCDATA.PATIENT_DIMENSION
group by RACE_C            ) a on (ov.c_dimcode = to_char(a.RACE_CD)) 
) a on (ov.c_dimcode = to_char(a.RACE_CD)) 
