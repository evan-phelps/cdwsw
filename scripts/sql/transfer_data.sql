--set serveroutput on;

--- stored procedure to populate dtdev using dtprd2
create or replace PROCEDURE transfer_data authid current_user
AS
    -- array of varchar2 type
    TYPE VARRAY_VARCHAR IS VARYING ARRAY(30) OF VARCHAR2(30);
    
    m_table VARCHAR2(60);
    m_query VARCHAR(5000);
    m_pat_tabs VARRAY_VARCHAR;
    m_vis_tabs VARRAY_VARCHAR;
    m_pat_detl VARRAY_VARCHAR;
    
BEGIN

    m_table := 'patient_ids_' || to_char(SYSDATE, 'YYYYMMDD');
    m_pat_tabs := VARRAY_VARCHAR( 'CDW.PATIENT','CDW.PATIENT_ID_MAP');
    m_vis_tabs := VARRAY_VARCHAR( 'CDW.VISIT', 'CDW.VISIT_DETAIL');
    m_pat_detl := VARRAY_VARCHAR( 'CDW.PROCEDURE', 'CDW.LAB_RESULT',
            'CDW.DIAGNOSIS', 'CDW.VITAL',
            'CDW.MEDICATION_ORDER', 'CDW.MEDICATION_ADMIN' );
            
    -- poppulate patient ids
    --DROP_TABLE_IF_EXIST(m_table);
    
    /*m_query := 'CREATE TABLE ' || m_table ||
      ' nologging AS    
      SELECT patient_id FROM cdw.patient@dtprd2link sample(10)
      WHERE rownum <= ' || v_npatients;*/
      m_query := 'CREATE TABLE ' || m_table ||
      ' nologging AS
      select patient_id from venkat.patient_20160413';
      DBMS_OUTPUT.PUT_LINE(m_query);
    
      
    EXECUTE immediate m_query;
    
/* -----------------------------
    -- part 1
*/
    SAVEPOINT start_transaction;
  
    BEGIN
    -- get patient, patient_id_map, visit, visit_detail for given id's
    FOR it IN 1 .. m_pat_tabs.count LOOP
      DBMS_OUTPUT.PUT_LINE(it || ': ' || m_pat_tabs(it));
      m_query := 'INSERT INTO ' || m_pat_tabs(it) || '(
          SELECT * FROM ' || m_pat_tabs(it) || '@dtprd2link
            WHERE patient_id IN
              (SELECT patient_id FROM ' || m_table ||') )';
      DBMS_OUTPUT.PUT_LINE(m_query);
      
      EXECUTE immediate m_query;
    END LOOP;
    
    -- get visit, visit_detail for given id's
    FOR it IN 1 .. m_vis_tabs.count LOOP
      DBMS_OUTPUT.PUT_LINE(it || ': ' || m_vis_tabs(it));
      m_query := 'INSERT INTO ' || m_vis_tabs(it) || '(
          SELECT * FROM ' || m_vis_tabs(it) || '@dtprd2link
            WHERE patient_id IN
              (SELECT patient_id FROM ' || m_table ||') )';
      DBMS_OUTPUT.PUT_LINE(m_query);
      
      EXECUTE immediate m_query;
    END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE ('Unexpected error in query: ' 
          || TO_CHAR(m_query));
        ROLLBACK TO start_transaction;
    END;

    m_query := 'DROP TABLE ' || m_table;
    EXECUTE immediate m_query;
    
    COMMIT;
/* ---------
   -- end part 1
*/ 

  --SAVEPOINT harmonize_patids;
 
 /* -----------------------------
    -- part 2
*/

  -- harmonize patient ids using euid
  BEGIN
    m_query := 'merge INTO cdw.patient_id_map pim USING cdw.patient pat 
      ON (pim.mpi_euid = pat.mpi_euid)
      WHEN matched THEN
        UPDATE SET  pim.patient_id = pat.patient_id';
    EXECUTE immediate m_query;
    DBMS_OUTPUT.PUT_LINE(m_query);
    
    m_table := 'vid_map_' || to_char(SYSDATE, 'YYYYMMDD');
    m_query := 'CREATE TABLE ' || m_table || ' nologging AS
      SELECT DISTINCT vd.visit_id, pim.patient_id FROM
        cdw.visit_detail vd, cdw.patient_id_map pim,
        cdw.visit v, cdw.patient pat
      WHERE (vd.visit_id = v.visit_id)
            AND (vd.htb_patient_id_ext = pim.mpi_lid)
            AND (pim.mpi_euid = pat.mpi_euid)
            AND (pim.mpi_systemcode    
              = DECODE(v.htb_enc_id_root, 
              ''2.16.840.1.113883.3.2489.2.1.2.1.3.1.2.4'', ''MUSC'', 
              ''2.16.840.1.113883.3.2489.2.1.2.2.3.1.2.2'', ''MUSC_EPIC'', 
              ''2.16.840.1.113883.3.2489.2.2.2.1.3.1.2.4'', ''GHS'', 
              ''2.16.840.1.113883.3.2489.2.3.4.1.2.4.1'', ''PH'', 
              ''2.16.840.1.113883.3.2489.2.3.4.1.2.4.3'', ''PH'',
              ''2.16.840.1.113883.3.2489.2.3.4.1.2.4.4'', ''PH'',
              ''2.16.840.1.113883.3.2489.2.3.4.1.2.4.2'', ''PH'', 
              ''2.16.840.1.113883.3.2489.2.4.4.1.2.4.1'', ''SRHS_R'', 
              ''2.16.840.1.113883.3.2489.2.4.4.1.2.4.2'', ''SRHS_S'', 
              ''2.16.840.1.113883.3.2489.2.4.4.1.2.4.3'', ''SRHS_V'', NULL)
            AND pat.patient_id IN (
              SELECT patient_id FROM cdw.patient))';
    DBMS_OUTPUT.PUT_LINE(m_query);
    EXECUTE immediate m_query;
    
    EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE ('Unexpected error occurred in harmonizing ids ');
    --ROLLBACK TO harmonize_patids;
    
    END;
/* ---------
   -- end part 2
*/ 
    
    COMMIT;
    
    SAVEPOINT match_visits;

 /* -----------------------------
    -- part 3
*/    

    -- match visits with newly harmonized patient ids
    BEGIN    
    
    FOR it IN 1 .. m_vis_tabs.count LOOP
      DBMS_OUTPUT.PUT_LINE(it || ': ' || m_vis_tabs(it));
      
      m_query := 'merge INTO ' || m_vis_tabs(it) || 'enc USING 
          ' || m_table || ' vpm ON (enc.visit_id = vpm.visit_id)
            WHEN matched THEN
            UPDATE SET enc.patient_id = vpm.patient_id';
      DBMS_OUTPUT.PUT_LINE(m_query);  
      EXECUTE immediate m_query;
      
      -- remove orphaned visits
      m_query := 'DELETE FROM ' || m_vis_tabs(it) || ' enc
          WHERE NOT EXISTS (
            SELECT 1 FROM cdw.patient pat 
            WHERE pat.patient_id = enc.patient_id)';
      DBMS_OUTPUT.PUT_LINE(m_query);  
      EXECUTE immediate m_query;
    END LOOP;
    
    m_query := 'DROP TABLE ' || m_table;
    EXECUTE immediate m_query;    
    
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE ('Unexpected error in query: ' 
          || TO_CHAR(m_query));
        ROLLBACK TO match_visits;
    END;
     
  COMMIT;
    
/* ---------
   -- end part 3
*/  

    SAVEPOINT patient_details;

 /* -----------------------------
    -- part 4
*/     
    m_table := 'vids_' || to_char(SYSDATE, 'YYYYMMDD');
    m_query := 'CREATE TABLE ' || m_table ||
      ' nologging AS
      select visit_id from cdw.visit';
    DBMS_OUTPUT.PUT_LINE(m_query);
    EXECUTE immediate m_query;

    BEGIN    
    
    FOR it IN 1 .. m_pat_detl.count LOOP
      DBMS_OUTPUT.PUT_LINE(it || ': ' || m_pat_detl(it));
      m_query := 'INSERT INTO ' ||  m_pat_detl(it) || ' (
        SELECT * FROM ' || m_pat_detl(it) || '@dtprd2link
          WHERE visit_id in (
            SELECT visit_id from ' || m_table || '))';
      DBMS_OUTPUT.PUT_LINE(m_query);  
      EXECUTE immediate m_query;
    END LOOP;
    
    m_query := 'DROP TABLE ' || m_table;
    EXECUTE immediate m_query;    
    
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE ('Unexpected error in query: ' 
          || TO_CHAR(m_query));
        ROLLBACK TO patient_details;
    END;
     
  COMMIT;
  
/* ---------
   -- end part 4
*/

END transfer_data;
/

/*  this part is to test transfer_data 

truncate table cdw.patient;
truncate table cdw.patient_id_map;
truncate table cdw.visit;
truncate table cdw.visit_detail;
truncate table cdw.procedure;
truncate table cdw.lab_result;
truncate table cdw.medication_order;
truncate table cdw.medication_admin;
/

begin
transfer_data;
end;
/
*/



