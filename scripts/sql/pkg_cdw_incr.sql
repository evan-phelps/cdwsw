-- ALTER SESSION SET current_schema = HSSC_ETL
-- /

/*
-- This database link should exist in the schema
-- of the package, or accessible from wherever the
-- package resides.
CREATE DATABASE LINK "MPI"
   CONNECT TO "PATIENT" IDENTIFIED BY <password>
   USING 'hssc-cdw-mpidb-d:1521/mpidev'
;
/
*/

DROP TABLE cdw_incr_mpi_cntrl;
CREATE TABLE cdw_incr_mpi_cntrl
  (
    BATCH_ID NUMBER NOT NULL,
    TIME_START DATE NOT NULL,
    TIME_LAST DATE,
    TRANS_TIME_START DATE,
    TRANS_TIME_LAST DATE,
    STATUS VARCHAR2(1 BYTE) NOT NULL
  )
/

DROP TABLE cdw_incr_mpi_stg;
CREATE TABLE cdw_incr_mpi_stg
  (
    BATCH_ID NUMBER NOT NULL,
    INSERT_DATE DATE DEFAULT SYSDATE NOT NULL,
    MPI_EUID VARCHAR2(25 BYTE), 
    FIRST_NAME VARCHAR2(250 BYTE), 
    MIDDLE_NAME VARCHAR2(250 BYTE), 
    LAST_NAME VARCHAR2(250 BYTE), 
    TITLE VARCHAR2(250 BYTE), 
    SUFFIX VARCHAR2(250 BYTE), 
    BIRTH_DATE DATE, 
    SEX_ORIG VARCHAR2(50 BYTE), 
    SSN VARCHAR2(50 BYTE), 
    MARITAL_STATUS_ORIG VARCHAR2(50 BYTE), 
    LANGUAGE_ORIG VARCHAR2(50 BYTE), 
    ETHNICITY_ORIG VARCHAR2(50 BYTE), 
    RACE_ORIG VARCHAR2(50 BYTE), 
    RELIGION_ORIG VARCHAR2(50 BYTE), 
    DEATH_DATE DATE, 
    DECEASED_IND VARCHAR2(5 BYTE), 
    ADDR_1 VARCHAR2(250 BYTE), 
    ADDR_2 VARCHAR2(250 BYTE), 
    ADDR_3 VARCHAR2(250 BYTE), 
    CITY VARCHAR2(250 BYTE), 
    STATE VARCHAR2(50 BYTE), 
    ZIP VARCHAR2(10 BYTE), 
    COUNTY VARCHAR2(50 BYTE), 
    COUNTRY VARCHAR2(50 BYTE), 
    HOME_PHONE VARCHAR2(250 BYTE), 
    WORK_PHONE VARCHAR2(250 BYTE), 
    MOBILE_PHONE VARCHAR2(250 BYTE), 
    EMAIL_ADDRESS VARCHAR2(250 BYTE), 
    MILITARY_STATUS_ORIG VARCHAR2(50 BYTE), 
    MILITARY_BRANCH VARCHAR2(100 BYTE), 
    MILITARY_RANK VARCHAR2(100 BYTE), 
    MILITARY_STATION VARCHAR2(250 BYTE) 
  )
/

/* General sequence for incremental batch processes.
*/
DROP SEQUENCE incr_batch_id_seq;
CREATE SEQUENCE incr_batch_id_seq
  MINVALUE 1 MAXVALUE 999999999999999999999999999
  INCREMENT BY 1 START WITH 1
  CACHE 20 NOORDER NOCYCLE
/

CREATE OR REPLACE
PACKAGE pkg_cdw_incr
IS
  PROCEDURE process_mpi_incr(
              p_trans_t0 cdw_incr_mpi_cntrl.trans_time_start%type DEFAULT NULL,
              p_max_trans_period NUMBER DEFAULT NULL);
END pkg_cdw_incr;
/

CREATE OR REPLACE
PACKAGE body pkg_cdw_incr
IS

PKG infolog.package_name%TYPE DEFAULT 'pkg_cdw_incr';

/* Absent an explicit date being provided as a parameter,
   an exception will be thrown if the transaction start
   time t0 < sysdate-IMPLICIT_MAX_DAYS */
IMPLICIT_MAX_DAYS NUMBER := 30;

/* User-defined error numbers must be in range [-20999,-20000]. */
ERRNUM_INCREMENTAL_TOO_BIG NUMBER := -20999;
ERRMSG_INCREMENTAL_TOO_BIG VARCHAR2(2048) := 'Incremental period is greater than specified maximum.';
/* If previous batch did not complete, raise exception. */
ERRNUM_LAST_INCOMPLETE NUMBER := -20998;
ERRMSG_LAST_INCOMPLETE VARCHAR2(2048) := 'The last batch did not complete';
/* If an unexpected state is detected */
ERRNUM_INCONSISTENCY NUMBER := -20997;
ERRMSG_INCONSISTENCY VARCHAR2(2048) := 'Unexpected state.';

/* Some state variables */
C_STAT_MPI_PREP CONSTANT VARCHAR2(1) := 'P';
C_STAT_MPI_STAGE CONSTANT VARCHAR2(1) := 'S';
C_STAT_MPI_MERGE CONSTANT VARCHAR2(1) := 'M';
C_STAT_MPI_MAPLIDS CONSTANT VARCHAR2(1) := 'L';
C_STAT_MPI_RECONCILE CONSTANT VARCHAR2(1) := 'R';
C_STAT_MPI_ERROR CONSTANT VARCHAR2(1) := 'E';
C_STAT_MPI_SUCCESS CONSTANT VARCHAR2(1) := 'C';

PROCEDURE process_mpi_incr(
    p_trans_t0 cdw_incr_mpi_cntrl.trans_time_start%type DEFAULT NULL,
    p_max_trans_period NUMBER DEFAULT NULL)
IS
  PRCDR infolog.procedure_name%TYPE DEFAULT 'process_mpi_incr';

  m_batch_id NUMBER := incr_batch_id_seq.NEXTVAL;
  m_batch_stat_last CHAR(1);
  m_time_start DATE := SYSDATE;
  m_trans_t0 DATE := p_trans_t0;
  m_trans_t1 DATE := m_time_start;
  m_batch_id_last NUMBER;
  m_max_trans_period NUMBER := p_max_trans_period;
  m_pkg VARCHAR2(100 BYTE) := 'PKG_CDW_INCR';
  m_prc VARCHAR2(100 BYTE) := 'PROCESS_MPI_INCR';
BEGIN

/* TODO: Consider having a separately scheduled process for recalculating
 *       all ages and ages at visits and any other time-dependent derived
 *       values... or, as policy, stop storing such values in CDW.
 *       Historically, these were calculated on every data refresh, but
 *       this does not make sense in an incremental scheme.
 */

/* Register batch in cntrl table.
 * TODO: Should interactions with the cntrl table be pulled into functions?
 *       Probably, yes, especially to allow updates to the cntrl table to
 *       escape the mainline SAVEPOINT/ROLLBACK scheme.  For now, I'll
 *       try to artfully avoid conflicts with the rollback strategy.
 * TODO: Consider decoupling the CDW_INCR_MPI_CNTRL table further to 
 *       allow for predefined batches with predetermined transaction
 *       periods.
 */

/* TODO: Add "trans time last" argument.  Currently, all batches process
 *       from "trans start time" through the latest transactions.
 * TODO: Also (or alternatively?) consider allowing start/end transaction
  *      numbers... or at lease store them?
 */
  INSERT INTO cdw_incr_mpi_cntrl (batch_id, time_start, time_last, status)
  VALUES (m_batch_id, m_time_start, SYSDATE, C_STAT_MPI_PREP)
  ;
  COMMIT;

/* If the transaction start time is not explicitly passed, then pick a
   starting point based on previous batch, if the previous batch was
   successful.
 */
  IF m_max_trans_period IS NULL THEN
    m_max_trans_period := IMPLICIT_MAX_DAYS;
  END IF;

  IF m_trans_t0 IS NULL THEN
    SELECT trans_time_last, status, batch_id
      INTO m_trans_t0, m_batch_stat_last, m_batch_id_last
    FROM
      (
        SELECT trans_time_last, status, batch_id
        FROM cdw_incr_mpi_cntrl
        WHERE batch_id < m_batch_id
        ORDER BY
          batch_id DESC
      )
    WHERE ROWNUM <= 1
    ;
    IF m_batch_stat_last != 'C' THEN
      RAISE_APPLICATION_ERROR(ERRNUM_LAST_INCOMPLETE,
                              ERRMSG_LAST_INCOMPLETE
                                || ' (#' || m_batch_id_last || ')');
    END IF;
  END IF;

  IF m_trans_t0 IS NULL THEN
    RAISE_APPLICATION_ERROR(ERRNUM_INCONSISTENCY,
                            ERRMSG_INCONSISTENCY
                              || ' TRANS_TIME_LAST is missing from last batch (#'
                              || m_batch_id_last || ').');
  END IF;
  IF sysdate-m_trans_t0 > m_max_trans_period THEN
    RAISE_APPLICATION_ERROR(ERRNUM_INCREMENTAL_TOO_BIG,
                            ERRMSG_INCREMENTAL_TOO_BIG
                            || ' Transaction period is '
                            || round(sysdate-m_trans_t0)
                            || ' days; max is '
                            || m_max_trans_period || ' days.');
  END IF;

/* Update cntrl information with transaction time range.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET trans_time_start = m_trans_t0,
      trans_time_last = m_trans_t1,
      time_last = SYSDATE,
      status = C_STAT_MPI_STAGE
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

/* Stage this batch of patient data from MPI.
 * TODO: Access SBYN tables over MPI (mpidev) DB link, which seems
 *       to currently be blocked by firewall rules.  In the meantime,
 *       I created empty SBYN tables in my own schema.
 */
  INSERT INTO cdw_incr_mpi_stg
  (
    BATCH_ID,
    MPI_EUID, 
    FIRST_NAME, 
    MIDDLE_NAME, 
    LAST_NAME, 
    TITLE, 
    SUFFIX, 
    BIRTH_DATE, 
    SEX_ORIG, 
    SSN, 
    MARITAL_STATUS_ORIG, 
    LANGUAGE_ORIG, 
    ETHNICITY_ORIG, 
    RACE_ORIG, 
    RELIGION_ORIG, 
    DEATH_DATE, 
    DECEASED_IND, 
    ADDR_1, 
    ADDR_2, 
    CITY, 
    STATE, 
    ZIP, 
    COUNTY, 
    COUNTRY, 
    HOME_PHONE, 
    WORK_PHONE, 
    MOBILE_PHONE, 
    MILITARY_STATUS_ORIG, 
    MILITARY_BRANCH, 
    MILITARY_RANK
  )
  SELECT DISTINCT
    m_batch_id,
    p.euid,
    p.firstname,    
    p.middlename,
    p.lastname,
    p.title,
    p.suffix,
    p.birthdate,
    p.gender,
    p.ssn,
    p.maritalstatus,
    p.language,
    p.ethnicity,
    p.race,
    p.religion,
    p.deathdate,
    p.deathindicator,
    a.addressline1,
    a.addressline2,
    a.city,
    a.statecode,
    a.postalcode,
    a.county,
    a.countrycode,
    h.phonenum home_num,
    w.phonenum work_num,
    m.phonenum mobile_num,
    p.militarystatus,
    p.militarybranch,
    p.militaryrank
  FROM SBYN_PATIENTSBR@MPI P
  LEFT JOIN SBYN_ADDRESSSBR@MPI A
    ON (    A.PATIENTID = P.PATIENTID
        AND A.ADDRESSTYPE='HOME')
  LEFT JOIN SBYN_PHONESBR@MPI H
    ON (    H.PATIENTID = P.PATIENTID
        AND H.PHONETYPE='HOME')
  LEFT JOIN SBYN_PHONESBR@MPI W
    ON (    W.PATIENTID = P.PATIENTID
        AND W.PHONETYPE='WORK')
  LEFT JOIN SBYN_PHONESBR@MPI M
    ON (   M.PATIENTID = P.PATIENTID
        AND M.PHONETYPE='MOBILE' )
  INNER JOIN sbyn_transaction@MPI T
    ON (    T.EUID = P.EUID
        AND T.TIMESTAMP >= m_trans_t0
        AND T.TIMESTAMP < m_trans_t1 )
  ;
  pkg_logging.log( p_package=>PKG,
                   p_procedure=>PRCDR,
                   p_message=>sql%rowcount || ' patients staged.',
                   p_parameters=>m_batch_id
                 );

/* Update cntrl information and simultaneously commit the staged records.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET time_last = SYSDATE,
      status = C_STAT_MPI_MERGE
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

/* Resolve HSSC_MPI harmonized codes into CDW normalized codes and upsert (merge)
 * records into patient table.
 */
  MERGE INTO cdw.patient pat
  USING (
    SELECT
      MPI_EUID, 
      FIRST_NAME, 
      MIDDLE_NAME,
      LAST_NAME,
      TITLE, 
      SUFFIX, 
      BIRTH_DATE,
      SSN, 
      DEATH_DATE, 
      DECEASED_IND,
      EMAIL_ADDRESS,
      ADDR_1,
      ADDR_2,
      CITY, 
      STATE,
      ZIP, 
      COUNTY, 
      COUNTRY, 
      HOME_PHONE,
      WORK_PHONE, 
      MOBILE_PHONE, 
      MILITARY_BRANCH,
      MILITARY_RANK,
      ETHNICITY_ORIG,
      MILITARY_STATUS_ORIG,
      MARITAL_STATUS_ORIG,
      LANGUAGE_ORIG, 
      RACE_ORIG, 
      RELIGION_ORIG,
      SEX_ORIG,
      r_ethn.tgt_code AS ETHNICITY,
      r_mlty.tgt_code AS MILITARY_STATUS,
      r_mrtl.tgt_code AS MARITAL_STATUS,
      r_lang.tgt_code AS LANGUAGE, 
      r_race.tgt_code AS RACE, 
      r_rlgn.tgt_code AS RELIGION,
      r_sex.tgt_code AS SEX,
      r_zip.tgt_code AS URBAN_RURAL
    FROM cdw_incr_mpi_stg stg
    LEFT OUTER JOIN cdwref.ref_demographics r_ethn
      ON (    r_ethn.src_code_type = 'HSSC_MPI_Ethnicity'
          AND stg.ethnicity_orig = r_ethn.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_mlty
      ON (    r_mlty.src_code_type = 'HSSC_MPI_MilitaryStatus'
          AND stg.military_status_orig = r_mlty.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_mrtl
      ON (    r_mrtl.src_code_type = 'HSSC_MPI_MaritalStatus'
          AND stg.marital_status_orig = r_mrtl.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_lang
      ON (    r_lang.src_code_type = 'HSSC_MPI_Language'
          AND stg.language_orig = r_lang.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_race
      ON (    r_race.src_code_type = 'HSSC_MPI_Race'
          AND stg.race_orig = r_race.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_rlgn
      ON (    r_rlgn.src_code_type = 'HSSC_MPI_Religion'
          AND stg.religion_orig = r_rlgn.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_sex
      ON (    r_sex.src_code_type = 'HSSC_MPI_Gender'
          AND stg.sex_orig = r_sex.src_code )
    LEFT OUTER JOIN cdwref.ref_demographics r_zip
      ON (    r_zip.src_code_type = 'ZIP'
          AND substr(stg.zip,1,5) = r_zip.src_code )
    WHERE stg.batch_id = m_batch_id
  ) pat_incr
  ON ( pat.mpi_euid = pat_incr.mpi_euid )
  WHEN MATCHED THEN UPDATE SET
    pat.FIRST_NAME = pat_incr.FIRST_NAME,
    pat.MIDDLE_NAME = pat_incr.MIDDLE_NAME,
    pat.LAST_NAME = pat_incr.LAST_NAME,
    pat.TITLE = pat_incr.TITLE,
    pat.SUFFIX = pat_incr.SUFFIX,
    pat.BIRTH_DATE = pat_incr.BIRTH_DATE,
    pat.SEX = pat_incr.SEX,
    pat.SEX_ORIG = pat_incr.SEX_ORIG,
    pat.SSN = pat_incr.SSN,
    pat.MARITAL_STATUS = pat_incr.MARITAL_STATUS,
    pat.MARITAL_STATUS_ORIG = pat_incr.MARITAL_STATUS_ORIG,
    pat.LANGUAGE = pat_incr.LANGUAGE,
    pat.LANGUAGE_ORIG = pat_incr.LANGUAGE_ORIG,
    pat.ETHNICITY = pat_incr.ETHNICITY,
    pat.ETHNICITY_ORIG = pat_incr.ETHNICITY_ORIG,
    pat.RACE = pat_incr.RACE,
    pat.RACE_ORIG = pat_incr.RACE_ORIG,
    pat.RELIGION = pat_incr.RELIGION,
    pat.RELIGION_ORIG = pat_incr.RELIGION_ORIG,
    pat.DEATH_DATE = pat_incr.DEATH_DATE,
    pat.DECEASED_IND = pat_incr.DECEASED_IND,
    pat.ADDR_1 = pat_incr.ADDR_1,
    pat.ADDR_2 = pat_incr.ADDR_2,
    pat.CITY = pat_incr.CITY,
    pat.STATE = pat_incr.STATE,
    pat.ZIP = pat_incr.ZIP,
    pat.COUNTY = pat_incr.COUNTY,
    pat.COUNTRY = pat_incr.COUNTRY,
    pat.URBAN_RURAL = pat_incr.URBAN_RURAL,
    pat.HOME_PHONE = pat_incr.HOME_PHONE,
    pat.WORK_PHONE = pat_incr.WORK_PHONE,
    pat.MOBILE_PHONE = pat_incr.MOBILE_PHONE,
    pat.EMAIL_ADDRESS = pat_incr.EMAIL_ADDRESS,
    pat.MILITARY_STATUS = pat_incr.MILITARY_STATUS,
    pat.MILITARY_STATUS_ORIG = pat_incr.MILITARY_STATUS_ORIG,
    pat.MILITARY_BRANCH = pat_incr.MILITARY_BRANCH,
    pat.MILITARY_RANK = pat_incr.MILITARY_RANK,
    pat.LAST_UPDATE_DATE = sysdate
  WHEN NOT MATCHED THEN INSERT (
    pat.MPI_EUID,
    pat.FIRST_NAME,
    pat.MIDDLE_NAME,
    pat.LAST_NAME,
    pat.TITLE,
    pat.SUFFIX,
    pat.BIRTH_DATE,
    pat.SEX,
    pat.SEX_ORIG,
    pat.SSN,
    pat.MARITAL_STATUS,
    pat.MARITAL_STATUS_ORIG,
    pat.LANGUAGE,
    pat.LANGUAGE_ORIG,
    pat.ETHNICITY,
    pat.ETHNICITY_ORIG,
    pat.RACE,
    pat.RACE_ORIG,
    pat.RELIGION,
    pat.RELIGION_ORIG,
    pat.DEATH_DATE,
    pat.DECEASED_IND,
    pat.ADDR_1,
    pat.ADDR_2,
    pat.CITY,
    pat.STATE,
    pat.ZIP,
    pat.COUNTY,
    pat.COUNTRY,
    pat.URBAN_RURAL,
    pat.HOME_PHONE,
    pat.WORK_PHONE,
    pat.MOBILE_PHONE,
    pat.EMAIL_ADDRESS,
    pat.MILITARY_STATUS,
    pat.MILITARY_STATUS_ORIG,
    pat.MILITARY_BRANCH,
    pat.MILITARY_RANK,
    pat.LAST_UPDATE_DATE
  ) VALUES (
    pat_incr.MPI_EUID,
    pat_incr.FIRST_NAME,
    pat_incr.MIDDLE_NAME,
    pat_incr.LAST_NAME,
    pat_incr.TITLE,
    pat_incr.SUFFIX,
    pat_incr.BIRTH_DATE,
    pat_incr.SEX,
    pat_incr.SEX_ORIG,
    pat_incr.SSN,
    pat_incr.MARITAL_STATUS,
    pat_incr.MARITAL_STATUS_ORIG,
    pat_incr.LANGUAGE,
    pat_incr.LANGUAGE_ORIG,
    pat_incr.ETHNICITY,
    pat_incr.ETHNICITY_ORIG,
    pat_incr.RACE,
    pat_incr.RACE_ORIG,
    pat_incr.RELIGION,
    pat_incr.RELIGION_ORIG,
    pat_incr.DEATH_DATE,
    pat_incr.DECEASED_IND,
    pat_incr.ADDR_1,
    pat_incr.ADDR_2,
    pat_incr.CITY,
    pat_incr.STATE,
    pat_incr.ZIP,
    pat_incr.COUNTY,
    pat_incr.COUNTRY,
    pat_incr.URBAN_RURAL,
    pat_incr.HOME_PHONE,
    pat_incr.WORK_PHONE,
    pat_incr.MOBILE_PHONE,
    pat_incr.EMAIL_ADDRESS,
    pat_incr.MILITARY_STATUS,
    pat_incr.MILITARY_STATUS_ORIG,
    pat_incr.MILITARY_BRANCH,
    pat_incr.MILITARY_RANK,
    sysdate
  )
  ;
  pkg_logging.log( p_package=>PKG,
                   p_procedure=>PRCDR,
                   p_message=>sql%rowcount
                              || ' patients merged into patient table.',
                   p_parameters=>m_batch_id
                 );

/* Update cntrl information and simultaneously commit the merged records.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET time_last = SYSDATE,
      status = C_STAT_MPI_MAPLIDS
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

  MERGE INTO cdw.patient_id_map pim
  USING (
   SELECT
      pat.patient_id, mpi_x.*
    FROM cdw_incr_mpi_stg stg
    INNER JOIN sbyn_enterprise@MPI mpi_x
      ON (    stg.mpi_euid = mpi_x.euid )
    INNER JOIN cdw.patient pat
      ON (    stg.mpi_euid = pat.mpi_euid )
    WHERE stg.batch_id = m_batch_id
  ) incr
  ON (     incr.lid = pim.mpi_lid
       AND incr.systemcode = pim.mpi_systemcode
     )
  WHEN MATCHED THEN UPDATE SET
    pim.MPI_EUID = incr.EUID,
    pim.LAST_UPDATE_DATE = sysdate
  WHEN NOT MATCHED THEN INSERT (
    pim.PATIENT_ID,
    pim.MPI_EUID,
    pim.MPI_LID,
    pim.MPI_SYSTEMCODE,
    pim.LAST_UPDATE_DATE
  ) VALUES (
    incr.PATIENT_ID,
    incr.EUID,
    incr.LID,
    incr.SYSTEMCODE,
    sysdate
  )
  ;
  pkg_logging.log( p_package=>PKG,
                   p_procedure=>PRCDR,
                   p_message=>sql%rowcount
                              || ' local patients mapped.',
                   p_parameters=>m_batch_id
                 );

/* Update cntrl information and simultaneously commit the euid-lid mapping.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET time_last = SYSDATE,
      status = C_STAT_MPI_RECONCILE
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

  MERGE INTO cdw.visit enc
  USING (
    SELECT DISTINCT
      vd.visit_id,
      pim.patient_id
    FROM
      cdw.visit_detail vd,
      cdw.patient_id_map pim,
      cdw.visit v,
      cdw_incr_mpi_stg incr
    WHERE vd.visit_id = v.visit_id
      AND vd.htb_patient_id_ext = pim.mpi_lid
      AND pim.mpi_systemcode = decode(v.htb_enc_id_root,
            '2.16.840.1.113883.3.2489.2.1.2.1.3.1.2.4', 'MUSC',
            '2.16.840.1.113883.3.2489.2.1.2.2.3.1.2.2', 'MUSC_EPIC',
            '2.16.840.1.113883.3.2489.2.2.2.1.3.1.2.4', 'GHS',
            '2.16.840.1.113883.3.2489.2.3.4.1.2.4.1', 'PH',
            '2.16.840.1.113883.3.2489.2.3.4.1.2.4.3', 'PH',
            '2.16.840.1.113883.3.2489.2.3.4.1.2.4.4', 'PH',
            '2.16.840.1.113883.3.2489.2.3.4.1.2.4.2', 'PH',
            '2.16.840.1.113883.3.2489.2.4.4.1.2.4.1', 'SRHS_R',
            '2.16.840.1.113883.3.2489.2.4.4.1.2.4.2', 'SRHS_S',
            '2.16.840.1.113883.3.2489.2.4.4.1.2.4.3', 'SRHS_V',
            NULL)
      AND pim.mpi_euid = incr.mpi_euid
      AND incr.batch_id = m_batch_id
  ) recs ON (    enc.visit_id = recs.visit_id)
  WHEN MATCHED THEN UPDATE SET 
    enc.patient_id = recs.patient_id
  WHERE enc.patient_id != recs.patient_id
  ;
  pkg_logging.log( p_package=>PKG,
                   p_procedure=>PRCDR,
                   p_message=>sql%rowcount
                              || ' visits updated with new patient_id.',
                   p_parameters=>m_batch_id
                 );

  MERGE INTO cdw.visit_detail vd
  USING (
    SELECT DISTINCT
      v.visit_id,
      pat.patient_id
    FROM
      cdw.patient pat,
      cdw.visit v,
      cdw_incr_mpi_stg incr
    WHERE v.patient_id = pat.patient_id
      AND pat.mpi_euid = incr.mpi_euid
      AND incr.batch_id = m_batch_id
  ) recs ON ( vd.visit_id = recs.visit_id )
  WHEN MATCHED THEN UPDATE SET 
    vd.patient_id = recs.patient_id
  WHERE vd.patient_id != recs.patient_id
  ;
  pkg_logging.log( p_package=>PKG,
                   p_procedure=>PRCDR,
                   p_message=>sql%rowcount
                              || ' visit details updated with new patient_id.',
                   p_parameters=>m_batch_id
                 );

/* Update cntrl information and simultaneously commit the updated patient ids
 * on visits.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET time_last = SYSDATE,
      status = C_STAT_MPI_SUCCESS
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

END process_mpi_incr;

END pkg_cdw_incr;
/
