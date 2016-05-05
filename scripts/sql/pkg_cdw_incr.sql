/*
CREATE DATABASE LINK "MPI"
   CONNECT TO "PATIENT" IDENTIFIED BY VALUES ':1'
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
    TRANS_TIME_START DATE NOT NULL,
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
  CACHE 20 NOORDER NOCYCLE ;
/

CREATE OR REPLACE
PACKAGE pkg_cdw_incr
IS
  PROCEDURE process_mpi_incr(
              p_trans_t0 cdw_incr_mpi_cntrl.trans_time_start%type DEFAULT NULL);
END pkg_cdw_incr;
/

CREATE OR REPLACE
PACKAGE body pkg_cdw_incr
IS

C_STAT_MPI_PREP CONSTANT VARCHAR2(1) := 'P';
C_STAT_MPI_STAGE CONSTANT VARCHAR2(1) := 'S';
C_STAT_MPI_MERGE CONSTANT VARCHAR2(1) := 'M';
C_STAT_MPI_MAPLIDS CONSTANT VARCHAR2(1) := 'L';
C_STAT_MPI_RECONCILE CONSTANT VARCHAR2(1) := 'R';
C_STAT_MPI_ERROR CONSTANT VARCHAR2(1) := 'E';
C_STAT_MPI_SUCCESS CONSTANT VARCHAR2(1) := 'C';

PROCEDURE process_mpi_incr(
    p_trans_t0 cdw_incr_mpi_cntrl.trans_time_start%type DEFAULT NULL)
IS
  m_batch_id NUMBER := incr_batch_id_seq.NEXTVAL;
  m_time_start DATE := SYSDATE;
  m_trans_t0 DATE := p_trans_t0;
  m_trans_t1 DATE := m_time_start;
  m_pkg VARCHAR2(100 BYTE) := 'PKG_CDW_INCR';
  m_prc VARCHAR2(100 BYTE) := 'PROCESS_MPI_INCR';
BEGIN

/* TODO: Consider action required on failure.  For example, if a batch
 *       fails, and if we allow new batches to be processed, then when
 *       we treat the root cause of the error, we would need to restrict
 *       re-processing of the errored batch to those records that do not
 *       have more recent updates for the same EUID.  There might be
 *       other scenarios to consider.
 */

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
 */
  INSERT INTO cdw_incr_mpi_cntrl (batch_id, time_start, time_last, status)
  VALUES (m_batch_id, m_time_start, SYSDATE, C_STAT_MPI_PREP)
  ;
  COMMIT;

/* Pick a starting point based on latest processed transaction time,
 * according to batch cntrl table.
 */
  IF m_trans_t0 IS NULL THEN
    SELECT trans_time_last INTO m_trans_t0
    FROM
      (
        SELECT trans_time_last
        FROM cdw_incr_mpi_cntrl
        ORDER BY
          trans_time_last DESC NULLS LAST
      )
    WHERE ROWNUM <= 1
    ;
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
  FROM SBYN_PATIENTSBR P
  LEFT JOIN SBYN_ADDRESSSBR A
    ON (    A.PATIENTID = P.PATIENTID
        AND A.ADDRESSTYPE='HOME')
  LEFT JOIN SBYN_PHONESBR H
    ON (    H.PATIENTID = P.PATIENTID
        AND H.PHONETYPE='HOME')
  LEFT JOIN SBYN_PHONESBR W
    ON (    W.PATIENTID = P.PATIENTID
        AND W.PHONETYPE='WORK')
  LEFT JOIN SBYN_PHONESBR M
    ON (   M.PATIENTID = P.PATIENTID
        AND M.PHONETYPE='MOBILE' )
  INNER JOIN sbyn_transaction T
    ON (    T.EUID = P.EUID
        AND T.TIMESTAMP >= m_trans_t0
        AND T.TIMESTAMP < m_trans_t1 )
  ;

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
  when not matched then insert (
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
  ) values (
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

/* Update cntrl information and simultaneously commit the merged records.
 */
  UPDATE cdw_incr_mpi_cntrl
  SET time_last = SYSDATE,
      status = C_STAT_MPI_MAPLIDS
  WHERE batch_id = m_batch_id
  ;
  COMMIT;

END process_mpi_incr;

END pkg_cdw_incr;
/
