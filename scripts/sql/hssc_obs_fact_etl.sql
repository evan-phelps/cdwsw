
-------------------------------------------------------------------------------------------------
create or replace PROCEDURE                            ETL_DIAGNOSIS authid current_user
IS

   m_rowcnt NUMBER := 0; -- row counter
   m_comrows NUMBER := 20000; -- commit every m_comrows
   m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_OBSERVATION';


  CURSOR dx_cur IS 
    select * from (
      SELECT  DISTINCT
            /*+ PARALLEL 4 */ 
        S.VISIT_DEID AS ENCOUNTER_NUM,
        S.PATIENT_DEID AS PATIENT_NUM,
        CASE dx_code_type WHEN 'ICD-10-CM'     THEN 'ICD10-CM:' || X.DX_CODE
              ELSE 'ICD9-CM:' || X.DX_CODE END AS CONCEPT_CD,
        COALESCE(X.DIAGNOSIS_DATE, V.VISIT_START_DATE) - 
            S.SHIFTVALUE AS START_DATE, -- modified 
        COALESCE(X.DX_SEQUENCE, 1) AS INSTANCE_NUM, 
        COALESCE(X.DIAGNOSIS_DATE, V.VISIT_END_DATE) - 
            S.SHIFTVALUE AS END_DATE,  -- modified
        X.DATASOURCE_ID as SOURCESYSTEM_CD, 
      rank () over (partition by visit_deid, dx_code, 
      COALESCE(X.DIAGNOSIS_DATE, V.VISIT_START_DATE) - S.SHIFTVALUE,  
      COALESCE(X.DX_SEQUENCE, 1) order by rownum) as rn -- modified
      FROM cdw.VISIT_DEID_MAP_HSSC@dtdev S
      JOIN cdw.DIAGNOSIS@dtdev X ON (S.VISIT_ID = X.VISIT_ID)
      JOIN CDW.VISIT@dtdev V on (S.VISIT_ID = V.VISIT_ID)
      where  (x.diagnosis_date is not null or 
          v.visit_start_date is not null)
      --AND S.VISIT_DEID IN ('920667', '1046912')  -- vsk test
    ) -- visit_id => visit_start_date --modified
    where rn=1;


  BEGIN


  for dx in  dx_cur LOOP
    BEGIN
      insert into i2b2hsscdata.observation_fact
        (encounter_num,patient_num,concept_cd,provider_id,start_date,
        end_date, modifier_cd,instance_num,valtype_cd,tval_char,nval_num,
        quantity_num,valueflag_cd,units_cd, location_cd,
        import_date,sourcesystem_cd,observation_blob,
        confidence_num,download_date,update_date)
       values
        (dx.encounter_num,dx.patient_num,dx.concept_cd,'@',dx.start_date,
          dx.end_date,'@',dx.instance_num, '@', '@', null,null, '@','@','@',
          sysdate,dx.sourcesystem_cd, null, '1',null,null);
          
          
      EXCEPTION WHEN OTHERS
      THEN
        pkg_error.log(p_error_code => substr(sqlerrm,1,9),
          p_error_message => substr(sqlerrm,12) || '.EXCP '
            || dx.encounter_num || ', ' || dx.patient_num,
          p_package => '', p_procedure => m_procname);
    END;
    m_rowcnt := m_rowcnt + 1;

    if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
      COMMIT;
    end if;
  end loop;
  
  commit;

END ETL_DIAGNOSIS;
-------------------------------------------------------------------------------------------------

create or replace PROCEDURE                   ETL_PROCEDURE authid current_user
IS

   m_rowcnt NUMBER := 0; -- row counter
   m_comrows NUMBER := 20000; -- commit every m_comrows
   m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_PROCEDURE';


  CURSOR px_cur IS 
      SELECT DISTINCT 
      /*+ PARALLEL 4 */
        S.VISIT_DEID as ENCOUNTER_NUM,
        S.PATIENT_DEID as PATIENT_NUM,
        CASE proc_code_type WHEN 'ICD-10-PCS' 
        THEN 'ICD10-PCS:' || X.PROC_CODE
        ELSE 'ICD9-PCS:' || X.PROC_CODE 
        END AS CONCEPT_CD,
    
        X.PROC_START_DATE - S.SHIFTVALUE as START_DATE,
        COALESCE(X.PROC_SEQUENCE, 1) as INSTANCE_NUM, 
  
        COALESCE(X.PROC_END_DATE, V.VISIT_END_DATE) - 
          S.SHIFTVALUE as END_DATE, -- PX Date or Visit End Date,  
        X.DATASOURCE_ID as SOURCESYSTEM_CD 
        FROM CDW.VISIT_DEID_MAP_HSSC@dtdev S
        JOIN CDW.PROCEDURE@dtdev X ON (S.VISIT_ID = X.VISIT_ID)
        JOIN CDW.VISIT@dtdev V ON (S.VISIT_ID = V.VISIT_ID)
        WHERE PROC_CODE_TYPE IN ('ICD-10-PCS','ICD-9-CM')

    /* **** 
        All dateless procedures currently (4/19/2016) 
        that have proc_codes also
        have dateful procedure with same proc_code.
      select count(1)
      from cdw.procedure px
      where proc_start_date is null
      and proc_code is not null
      and exists (select 1 from cdw.procedure px2
            where px2.visit_id = px.visit_id
              and px2.proc_code = px.proc_code
              and px2.proc_start_date is not null
              )
;
*/
AND x.proc_start_date is not null and x.proc_code is not null;


  BEGIN


  for px in  px_cur LOOP
    BEGIN
    insert into i2b2hsscdata.observation_fact (
      encounter_num,patient_num,concept_cd,provider_id,start_date,
      end_date, modifier_cd,instance_num,valtype_cd,tval_char,nval_num,
      quantity_num,valueflag_cd,units_cd, location_cd,import_date,
      sourcesystem_cd,observation_blob, 
      confidence_num,download_date,update_date)
       values
        (px.encounter_num,px.patient_num,px.concept_cd,'@',px.start_date,
         px.end_date,'@',px.instance_num,
        '@', '@', null, null, '@', '@', '@', sysdate, 
        px.sourcesystem_cd, null, '1' , null, null);
                    
          EXCEPTION WHEN OTHERS 
          THEN
          pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP ' 
              || px.encounter_num || ', ' || px.patient_num,
              p_package => '', p_procedure => m_procname);
    END;

        m_rowcnt := m_rowcnt + 1;
        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
              COMMIT;
        end if;
  end loop;
  
  commit;

END ETL_PROCEDURE;
-------------------------------------------------------------------------------------------------

create or replace PROCEDURE                   ETL_LABS authid current_user
IS

   m_rowcnt NUMBER := 0; -- row counter
   m_comrows NUMBER := 40000; -- commit every m_comrows
   m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_LABS';


  cursor labs_cur is
    SELECT DISTINCT 
      /*+ PARALLEL 4 */
        S.VISIT_DEID as ENCOUNTER_NUM,
        S.PATIENT_DEID as PATIENT_NUM,
      'LOINC:' || X.LOINC_CD as CONCEPT_CD, --how do we pull RZ.TGT_CODE_TYPE?
    
      COALESCE(X.RESULT_DT, V.VISIT_START_DATE) - S.SHIFTVALUE as START_DATE, -- Lab Date or Visit Start Date -- modified
      '1' as INSTANCE_NUM,
    
      COALESCE(X.result_DT, V.VISIT_END_DATE) - S.SHIFTVALUE as END_DATE, -- Lab Date or Visit End Date,  -- modified
    
      X.DATASOURCE_ID as SOURCESYSTEM_CD
      FROM cdw.VISIT_DEID_MAP_HSSC@dtdev S
      JOIN cdw.VISIT@dtdev V ON (V.VISIT_ID = S.VISIT_ID)
      JOIN cdw.lab_result@dtdev X ON (V.HTB_ENC_ACT_ID = X.HTB_ENC_ACT_ID)

      -- remove ones without LOINC codes
      WHERE X.LOINC_CD is not null; -- added 4/19/2016


  BEGIN


    for lb in  labs_cur LOOP
          BEGIN
           insert into i2b2hsscdata.observation_fact
           (encounter_num,patient_num,concept_cd,provider_id,start_date,
             end_date, modifier_cd,instance_num,valtype_cd,tval_char,
             nval_num,quantity_num,valueflag_cd,units_cd, location_cd,
             import_date,sourcesystem_cd,observation_blob,
             confidence_num,download_date,update_date)
          values (lb.encounter_num,lb.patient_num,lb.concept_cd,
          '@',lb.start_date,lb.end_date,'@',lb.instance_num, '@', '@',
          null, null, '@', '@', '@',sysdate,lb.sourcesystem_cd,
          null, '1', null, null);
        
          EXCEPTION WHEN OTHERS
          THEN
          pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP '
              || lb.encounter_num || ', ' || lb.patient_num,
              p_package => '', p_procedure => m_procname);
        END;

        m_rowcnt := m_rowcnt + 1;
        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
            COMMIT;
        end if;

  end loop;
  
  commit;

END ETL_LABS;
-------------------------------------------------------------------------------------------------
create or replace PROCEDURE            "ETL_MEDICATION_ORDER" authid current_user
IS

   m_rowcnt NUMBER := 0; -- row counter
   m_comrows NUMBER := 20000; -- commit every m_comrows
   m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_MEDICATION_ORDER';


 CURSOR med_orders_cur IS
      select * from (
    select distinct nvl2(a.admin_start_date,'A','N') AS admin,
       v.visit_deid, v.patient_deid,
            COALESCE(a.admin_start_date, o.START_DATE, s.visit_start_date) -
                v.shiftvalue as shifted_start_date, -- modified
            COALESCE(a.admin_end_date, o.END_DATE, s.visit_end_date) -
                v.shiftvalue as shifted_end_date, -- modified
            o.med_code,
            rank () over (
              partition by v.visit_deid,
                  coalesce(to_char(a.admin_start_date,'mm/dd/yyyy'),to_char(o.start_date,'mm/dd/yyyy')),
                  o.med_code order by rownum) as rank
          from cdw.medication_order@dtdev o
          left outer join cdw.medication_admin@dtdev a on (o.MED_ORDER_ID = a.MED_ORDER_ID)
          join cdw.visit_deid_map_HSSC@dtdev v on (v.visit_id = o.visit_id)
          join cdw.visit@dtdev s on (v.visit_id = s.visit_id)
          where o.visit_id is not null
       
          -- 1% of med_orders the start date is slightly off. we will revisit this
          -- for the next round (4/19/2016)
          and (a.admin_start_date is not null or o.start_date is not null or s.visit_start_date is not null)
          and o.med_code is not null
          order by v.visit_deid, shifted_start_date, o.med_code
      )
      where rank=1;

  BEGIN


    for mo in  med_orders_cur LOOP

        if mo.med_code is not null then
          BEGIN
            insert into I2B2HSSCDATA.observation_fact
            (encounter_num,patient_num,concept_cd,provider_id,start_date,
             end_date, modifier_cd,instance_num,valtype_cd,tval_char,
             valueflag_cd,units_cd, location_cd,import_date,sourcesystem_cd)
            values (mo.visit_deid,mo.patient_deid,'RXCUI:' || mo.med_code,'@',
            mo.shifted_start_date, mo.shifted_end_date, '@',mo.rank,
            '@','@','@','@','@',sysdate,'CDW');

          if mo.admin = 'A' then
             insert into I2B2HSSCDATA.observation_fact
             (encounter_num,patient_num,concept_cd, provider_id,start_date,
               end_date, modifier_cd,instance_num,valtype_cd,tval_char,
               valueflag_cd,units_cd, location_cd,import_date,sourcesystem_cd)
                values (mo.visit_deid,mo.patient_deid,'RXCUI:' || mo.med_code,
                '@', mo.shifted_start_date, mo.shifted_end_date,
                'MED:ADMIN',mo.rank,'T',mo.admin,NULL,'@','@',sysdate,'CDW');
          end if;

          EXCEPTION WHEN OTHERS
          THEN
            pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP '
              || mo.visit_deid || ', ' || mo.patient_deid,
              p_package => '', p_procedure => m_procname);

        END;
        m_rowcnt := m_rowcnt + 1;

        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
              COMMIT;
        end if;

    end if;
  end loop;

  commit;

END ETL_MEDICATION_ORDER;
-------------------------------------------------------------------------------------------------


create or replace PROCEDURE                   ETL_VITAL authid current_user
IS

   m_rowcnt NUMBER := 0; -- row counter
   m_comrows NUMBER := 40000; -- commit every m_comrows
   m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_VITAL';


  cursor vital_cur is 
    SELECT DISTINCT
    /*+ PARALLEL 8 */
      D.PATIENT_DEID AS PATIENT_NUM,
      M.VISIT_DEID AS ENCOUNTER_NUM,
      'VITAL:' || decode(V.OBSERVATION_TYPE,
        'HEIGHT','HT','WEIGHT','WT') as CONCEPT_CD,
  
      COALESCE(V.COLLECTION_DATE, S.VISIT_START_DATE) -
          D.SHIFTVALUE as START_DATE,
      COALESCE(V.COLLECTION_DATE, S.VISIT_END_DATE) -
          D.SHIFTVALUE as END_DATE,

      '1' as INSTANCE_NUM,
      '@' as PROVIDER_ID,

        -- convert height to inches and weight to pounds
        -- todo look at the unit of measure (don't assume it's cm/kg)
        ROUND(decode(v.observation_type,'HEIGHT',
          V.VITAL_VALUE_NUM * .3937,V.VITAL_VALUE_NUM * 2.2046),2)
          as NVAL_NUM,

      '@' as VALUEFLAG_CD,
      null as QUANTITY_NUM,
      DECODE(V.OBSERVATION_TYPE,'HEIGHT','inches','lbs') as UNITS_CD,
    'N' as valtype_cd,
    'E' as tval_char---,
    ----V.HTB_ENC_ACT_ID as ACT_ID,
    ----V.HTB_ENC_ACT_VER_NUM AS ACT_VER_NUM
    FROM cdw.VITAL@dtdev V
    JOIN CDW.VISIT@dtdev S ON (S.HTB_ENC_ACT_ID = V.HTB_ENC_ACT_ID)
    JOIN cdw.Patient_deid_map_hssc@dtdev D ON (S.PATIENT_ID = D.PATIENT_ID)
    JOIN cdw.Visit_deid_map_hssc@dtdev M ON (M.VISIT_ID = S.VISIT_ID)

    -- upper case OBSERVATION_TYPE required ? -- revisit (04/19/2016)
    where OBSERVATION_TYPE in ('HEIGHT','WEIGHT')
    and (v.collection_date is not null or s.visit_start_date is not null);

  BEGIN


    FOR vo IN vital_cur LOOP
        BEGIN
            insert into i2b2hsscdata.observation_fact (
              start_date, end_date, provider_id, patient_num,
              instance_num, import_date, encounter_num,
              concept_cd,nval_num,units_cd, valtype_cd,
              tval_char, update_date,modifier_cd)
            values (vo.start_date, vo.end_date,'@',
            vo.patient_num, vo.instance_num, sysdate,
            vo.encounter_num, vo.concept_cd,
            vo.nval_num,vo.units_cd,vo.valtype_cd,vo.tval_char,sysdate,'@');

        EXCEPTION WHEN OTHERS
        THEN
          pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP '
              || vo.encounter_num || ', ' || vo.patient_num,
              p_package => '', p_procedure => m_procname);
        END;
        m_rowcnt := m_rowcnt + 1;

        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
              DBMS_OUTPUT.PUT_LINE('committed: ' || m_rowcnt);
              COMMIT;
        end if;

  end loop;

  commit;

END ETL_VITAL;

-------------------------------------------------------------------------------------------------

create or replace PROCEDURE                   ETL_PATIENT authid current_user
IS

  m_rowcnt NUMBER := 0; -- row counter
  m_comrows NUMBER := 40000; -- commit every m_comrows
  m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_VITAL';

  CURSOR p_cur IS
    SELECT DISTINCT
      /*+ PARALLEL 4 */
      M.PATIENT_DEID as PATIENT_NUM,
      P.BIRTH_DATE-M.SHIFTVALUE AS SHIFTED_BIRTH_DATE, -- null-shiftvalue=null
      P.DEATH_DATE-M.SHIFTVALUE  AS SHIFTED_DEATH_DATE,
      floor(months_between(coalesce(P.death_date-M.SHIFTVALUE,sysdate),
        birth_date-m.shiftvalue)/12) as SHIFTED_AGE,
                              -- sysdate => stop ageing at death
      COALESCE(P.SEX, 'Unknown') AS SEX,
      COALESCE(P.SEX_ORIG, 'Unknown') AS SEX_ORIG,
      COALESCE(P.RACE, 'Unknown') AS RACE,
      COALESCE(P.RACE_ORIG, 'Unknown') AS RACE_ORIG,
      COALESCE(P.ETHNICITY, 'Unknown') AS ETHNICITY,
      COALESCE(P.ETHNICITY_ORIG, 'Unknown') AS ETHNICITY_ORIG,
      COALESCE(P.MILITARY_STATUS, 'Unknown') AS MILITARY_STATUS,
      COALESCE(P.MILITARY_STATUS_ORIG, 'Unknown') AS MILITARY_STATUS_ORIG,
      SUBSTR(P.ZIP,1,3) as ZIP,
      CASE WHEN P.DECEASED_IND = 'Y' THEN 'D' ELSE 'L' END as VITAL_STATUS_CD,
      P.URBAN_RURAL
      FROM CDW.PATIENT_DEID_MAP_HSSC@dtdev M
      JOIN CDW.PATIENT@dtdev P on (P.PATIENT_ID = M.PATIENT_ID);

  BEGIN


    FOR po IN p_cur LOOP
        BEGIN

          insert into i2b2hsscdata.patient_dimension 
            (patient_num,birth_date,sex_cd,sex_cd_orig,ethnicity_cd,
            ethnicity_cd_orig, race_cd, race_cd_orig, import_date,
            death_date,vital_status_cd,update_date,age_in_years_num,
            military_status_cd, military_status_orig, zip_cd,urban_rural_cd) 
          values (po.patient_num, po.shifted_birth_date,po.sex,po.sex_orig,
            po.ethnicity, po.ethnicIty_orig, po.race,po.race_orig,
            sysdate, po.shifted_death_date, po.vital_status_cd,sysdate,
            po.shifted_age,po.military_status, po.military_status_orig, 
            po.zip,po.urban_rural);


        EXCEPTION WHEN OTHERS
        THEN
          pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP '
              || po.patient_num, p_package => '',
              p_procedure => m_procname);
        END;
        m_rowcnt := m_rowcnt + 1;

        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
              DBMS_OUTPUT.PUT_LINE('committed: ' || m_rowcnt);
              COMMIT;
        end if;

  end loop;

  commit;

END ETL_PATIENT;

-------------------------------------------------------------------------------------------------

create or replace PROCEDURE  ETL_VISIT authid current_user
IS

  m_rowcnt NUMBER := 0; -- row counter
  m_comrows NUMBER := 40000; -- commit every m_comrows
  m_procname ERRORLOG.PROCEDURE_NAME%TYPE := 'ETL_VITAL';

  CURSOR v_cur IS

  SELECT 
      /*+ PARALLEL 4 */
    X.VISIT_DEID as ENCOUNTER_NUM,
    X.PATIENT_DEID as PATIENT_NUM,
    CASE WHEN S.STATUS = 'active' THEN 'A' WHEN 
    S.STATUS = 'completed' THEN 'F' ELSE NULL END AS ACTIVE_STATUS_CD,
    S.VISIT_START_DATE - X.SHIFTVALUE as START_DATE, 
    S.VISIT_END_DATE - X.SHIFTVALUE as END_DATE, S.CLASS_CODE,
    S.CLASS_CODE_ORIG,S.TYPE_CODE, S.TYPE_CODE_ORIG,
    floor(months_between(s.visit_start_date - x.shiftvalue, 
      p.birth_date - x.shiftvalue)/12) SHIFTED_AGE_AT_VISIT,
    I.ACCOM_CODE,I.ACCOM_CODE_ORIG,
    I.ADMISSION_SOURCE,I.ADMISSION_SOURCE_ORIG,
    I.ADMISSION_TYPE, I.ADMISSION_TYPE_ORIG,
    I.DISCHARGE_DISPOSITION, I.DISCHARGE_DISPOSITION_ORIG,
    I.FINANCIAL_CLASS_GROUP, I.FINANCIAL_CLASS_ORIG,
    I.HOSPITAL_SERVICE_GROUP, I.HOSPITAL_SERVICE_ORIG,
    S.LOS, S.DATASOURCE_ID, I.LAST_UPDATE_DATE
    FROM CDW.VISIT@dtdev S
    JOIN CDW.VISIT_DEID_MAP_HSSC@dtdev X ON (X.VISIT_ID = S.VISIT_ID)
    JOIN CDW.VISIT_DETAIL@dtdev I ON (S.VISIT_ID = I.VISIT_ID)
    join cdw.patient@dtdev p on (s.patient_id = p.patient_id)
    where s.visit_start_date is not null;
  
  BEGIN

    FOR vo IN v_cur LOOP
        BEGIN
          insert into i2b2hsscdata.visit_dimension 
            (PATIENT_NUM,ENCOUNTER_NUM,ACTIVE_STATUS_CD,
            START_DATE,END_DATE,INOUT_CD,INOUT_CD_ORIG,TYPE_CD,
            TYPE_CD_ORIG,AGE_AT_VISIT,ACCOMM_CD,ACCOMM_CD_ORIG,
            ADMISSION_SOURCE,ADMISSION_SOURCE_ORIG,ADMISSION_TYPE,
            ADMISSION_TYPE_ORIG,DISCH_DISP,DISCH_DISP_ORIG,
            FIN_CLASS_GROUP,FIN_CLASS_ORIG,HOSP_SRV_GROUP,HOSP_SRV_ORIG,
            LENGTH_OF_STAY,IMPORT_DATE,SOURCESYSTEM_CD,UPDATE_DATE) 
          values (vo.patient_num,vo.encounter_num,vo.active_status_cd,
            vo.start_date,vo.end_date,vo.class_code,vo.class_code_orig,
            vo.type_code,vo.type_code_orig,vo.shifted_age_at_visit,
            vo.accom_code,vo.accom_code_orig,vo.admission_source,
            vo.admission_source_orig,vo.admission_type,vo.admission_type_orig,
            vo.discharge_disposition,vo.discharge_disposition_orig,
            vo.financial_class_group,vo.financial_class_orig,
            vo.hospital_service_group,vo.hospital_service_orig,vo.los,
            sysdate,vo.datasource_id,vo.last_update_date);


        EXCEPTION WHEN OTHERS
        THEN
          pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '.EXCP '
              || vo.encounter_num || ', ' || vo.patient_num,
              p_procedure => m_procname);
        END;
        m_rowcnt := m_rowcnt + 1;

        if ( m_rowcnt > 0 and mod(m_rowcnt, m_comrows) = 0 ) then
              DBMS_OUTPUT.PUT_LINE('committed: ' || m_rowcnt);
              COMMIT;
        end if;

  end loop;

  commit;

END ETL_VISIT;
-------------------------------------------------------------------------------------------------


create or replace PROCEDURE          HSSC_OBS_FACT authid current_user 
IS
  BEGIN
    HSSC_ETL.ETL_PATIENT();
    HSSC_ETL.ETL_VISIT();
    HSSC_ETL.ETL_DIAGNOSIS();
    HSSC_ETL.ETL_PROCEDURE();
    HSSC_ETL.ETL_LABS();
    HSSC_ETL.ETL_MEDICATION_ORDER();
    HSSC_ETL.ETL_VITAL();
END;

-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------

