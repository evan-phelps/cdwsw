--------------------------------------------------------
--  File created - Thursday-April-14-2016   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure LOAD_DEID_TABLES
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDW"."LOAD_DEID_TABLES" 
IS
BEGIN 
  -- Do we need table backups?
   execute immediate 'Truncate table cdw.visit_deid_map_hssc'; --truncate visit_deid_map_hssc table
    execute immediate 'Truncate table cdw.patient_deid_map_hssc'; --truncate patient_deid_map_hssc table
    
    execute immediate 'Alter table cdw.visit_deid_map_hssc drop constraint pk_visit_deid_map_hssc'; --drop primary key for visit_deid_map_hssc
    execute immediate 'Drop index cdw.visit_deid_map_hssc'; -- drop index for visit_deid_map_hssc
   
   execute immediate 'Alter table cdw.patient_deid_map_hssc drop constraint pk_patient_deid_map_hssc'; --drop primary key patient_deid map_hssc
    execute immediate 'Drop index cdw.patient_deid_map_hssc'; -- drop index for patient_deid map_hssc
  --Decided to let the sequence run instead of dropping and 
-- recreating it every time
    --execute immediate 'Drop sequence cdw.visit_seq_deid_hssc'; --drop exisiting seq object for patient_deid
   -- execute immediate 'CREATE SEQUENCE CDW.VISIT_SEQ_DEID_HSSC INCREMENT BY 1 START WITH 1001 MAXVALUE 9999999999999999999999999999 NOMINVALUE NOCYCLE CACHE 1000 NOORDER'; --create new seq object for visit_deid 
   -- execute immediate 'Drop sequence CDW.PATIENT_SEQ_DEID_HSSC'; --drop exisiting seq object for patient_deid
--    execute immediate 'CREATE SEQUENCE CDW.PATIENT_SEQ_DEID_HSSC 
--    INCREMENT BY 1 
--    START WITH 1001 
--    MAXVALUE 9999999999999999999999999999 
--    NOMINVALUE
--    NOCYCLE 
--    CACHE 1000 
--    NOORDER';
      
   
   
    INSERT INTO CDW.PATIENT_DEID_MAP_HSSC (PATIENT_DEID, PATIENT_ID) SELECT CDW.PATIENT_SEQ_DEID_HSSC.NEXTVAL, P.PATIENT_ID FROM CDW.PATIENT P WHERE P.PATIENT_ID IN (SELECT PATIENT_ID FROM CDW.VISIT);
    
    insert into cdw.visit_deid_map_hssc (visit_id, visit_deid, patient_deid) select
v.visit_id, CDW.VISIT_SEQ_DEID_HSSC.nextval, p.patient_deid from
cdw.visit v join cdw.patient_deid_map_hssc p on (v.patient_id = p.patient_id)
;
    
    execute immediate 'Create index cdw.visit_deid_map_hssc on cdw.visit_deid_map_hssc (visit_deid)'; -- add index on visit_deid for visit_deid_map_hssc
    execute immediate 'Alter table cdw.visit_deid_map_hssc add constraint pk_visit_deid_map_hssc primary key  (visit_deid) using index'; --add primary key on visit_deid for visit_deid_map_hssc
    execute immediate 'Create index cdw.patient_deid_map_hssc on cdw.patient_deid_map_hssc (patient_deid)'; --  add index on patient_deid for patient_deid map_hssc
    execute immediate 'Alter table cdw.patient_deid_map_hssc add constraint pk_patient_deid_map_hssc primary key (patient_deid) using index'; --add primary key on patient_deid for patient_deid map_hssc
commit;
END LOAD_DEID_TABLES;

/
