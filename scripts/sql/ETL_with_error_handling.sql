/*
 Created an example use-case of adding error_log into an ETL
 The procedure below is portion extracted out 
 of POP_HSSC_OBS to test pkg_error logging of error message
 select * from venkat.errorlog;
 output looks like:

22    18-APR-16 16:53:20    ORA-00001    unique constraint (VENKAT.OBSERVATION_FACT_PK) violated. Duplicate found at 1046912, 39451        VK_HSSCOBS        
23    18-APR-16 16:53:20    ORA-00001    unique constraint (VENKAT.OBSERVATION_FACT_PK) violated. Duplicate found at 1046912, 39451        VK_HSSCOBS   
25    18-APR-16 16:53:20    ORA-00001    unique constraint (VENKAT.OBSERVATION_FACT_PK) violated. Duplicate found at 1046912, 39451        VK_HSSCOBS        
27    18-APR-16 16:53:20    ORA-00001    unique constraint (VENKAT.OBSERVATION_FACT_PK) violated. Duplicate found at 1046912, 39451        VK_HSSCOBS        
*/

create or replace PROCEDURE VK_HSSCOBS authid current_user
IS

   cnt number := 0;
   v_procedure ERRORLOG.PROCEDURE_NAME%TYPE := 'VK_HSSCOBS';

   CURSOR px_cur IS
   SELECT DISTINCT 
    /*+ PARALLEL 4 */
    S.VISIT_DEID as ENCOUNTER_NUM,
    S.PATIENT_DEID as PATIENT_NUM,
    CASE proc_code_type WHEN 'ICD-10-PCS'     THEN 'ICD10-PCS:' || X.PROC_CODE
                        ELSE 'ICD9-PCS:' || X.PROC_CODE END AS CONCEPT_CD,
    COALESCE(X.PROC_START_DATE, S.SHIFTED_START_DATE, SYSDATE) as START_DATE, -- PX Date or Visit Start Date
    COALESCE(X.PROC_SEQUENCE, 1) as INSTANCE_NUM, 
    COALESCE(X.PROC_END_DATE, S.SHIFTED_END_DATE, SYSDATE) as END_DATE, -- PX Date or Visit End Date,  
    X.DATASOURCE_ID as SOURCESYSTEM_CD 
FROM CDW.VISIT_DEID_MAP_HSSC@dtdev S
JOIN CDW.PROCEDURE@dtdev X ON (S.VISIT_ID = X.VISIT_ID)
WHERE PROC_CODE_TYPE IN ('ICD-10-PCS','ICD-9-CM') AND
S.VISIT_DEID IN ('920667', '1046912');

BEGIN

for px in  px_cur LOOP
begin
 insert into VENKAT.observation_fact (encounter_num,patient_num,concept_cd,provider_id,start_date,end_date,
        modifier_cd,instance_num,valtype_cd,tval_char,nval_num,quantity_num,valueflag_cd,units_cd, location_cd,import_date,sourcesystem_cd,observation_blob,
        confidence_num,download_date,update_date)
       values
        (px.encounter_num,px.patient_num,px.concept_cd,'@',px.start_date,px.end_date,'@',px.instance_num,
        '@', '@', null, null, '@', '@', '@', sysdate, px.sourcesystem_cd, null, '1' , null, null);
        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            DBMS_OUTPUT.PUT_LINE('Duplicate found at' || px.encounter_num || ', ' || px.patient_num );

            -- when a foreign key constraint is violated, caught an exception and logged the error
            pkg_error.log(p_error_code => substr(sqlerrm,1,9),
              p_error_message => substr(sqlerrm,12) || '. Duplicate found at ' || px.encounter_num || ', ' || px.patient_num,
              p_package => '', p_procedure => v_procedure/*, p_location => v_location*/);
            
end;


/*        if (cnt > 20000) then
              COMMIT;
              cnt := 0;
            end if;*/
  end loop;

END;
