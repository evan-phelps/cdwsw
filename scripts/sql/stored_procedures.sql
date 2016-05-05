------------------------------------------------------------------------------------------------------
-- procedure   : drop_table_if_exist
-- input       : table name
-- description : drops a table in the current user schema if table exists

create or replace PROCEDURE  DROP_TABLE_IF_EXIST( m_tabname IN VARCHAR,
  m_user IN VARCHAR DEFAULT USER) AS
  m_count  PLS_INTEGER := -1;
  m_sql VARCHAR(200);

BEGIN
  --DBMS_OUTPUT.PUT_LINE('user = ' || m_user || ' table = ' || m_tabname);
  BEGIN
    SELECT COUNT(1) INTO m_count  FROM ALL_TABLES
      WHERE TABLE_NAME = m_tabname AND OWNER = m_user;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        m_count := NULL;
  END;

  --DBMS_OUTPUT.PUT_LINE('m_count = ' || m_count);
  
  IF m_count > 0 THEN
    m_sql := 'DROP TABLE ' || m_tabname;
    EXECUTE IMMEDIATE m_sql;
   -- DBMS_OUTPUT.PUT_LINE('Dropping table ' || m_tabname);
  ELSE
    DBMS_OUTPUT.PUT_LINE('No table found matching ''' || m_tabname || '''');
  END IF;

END DROP_TABLE_IF_EXIST;

-- end procedure drop_table_if_exist

-- procedure   : i2b2_aggregates
-- input       : version (e.g., 'YYYYMMDD')
-- description : aggregegate counts of i2b2 instance

create or replace PROCEDURE                                     i2b2_aggregates(m_version IN VARCHAR
DEFAULT TO_CHAR(SYSDATE, 'YYYYMMDD') )  authid current_user
AS
  -- cursor to fetch i2b2*data instances
  m_cur SYS_REFCURSOR;
 
  -- placeholders for dynamic query strings
  m_query VARCHAR2(200);
  t_query VARCHAR(2000);
 
  -- username for i2b2 instance
  m_username varchar(100);
  
  -- suffix reflects table version
  m_suffix varchar(100);
  
  -- tables to hold counts
  t_obs_fact varchar(100);
  t_obs_dtype varchar(100);
  t_summary varchar(100);
  
 
BEGIN
 
  -- get distinct usernames from dba_users table matching 'I2B2*DATA'
  m_query:='select distinct(username) from dba_users where username like ''I2B2%DATA''';
 
  -- open cursor and loop over instances (usernames)
  OPEN m_cur FOR m_query;
  LOOP
 
    FETCH m_cur INTO m_username;
    EXIT WHEN m_cur%NOTFOUND;
    
    -- prompt current user being processed
    DBMS_OUTPUT.PUT_LINE('Extracting counts from ' || m_username);
    
    -- create a suffix for tables which includes versioning
    -- we know the first 4 letters are I2B2 and last 4 are DATA.
    -- so trim the string to extract substring between these
    -- and make sure version is small so we can stay within 30 chars for table names
    m_suffix := substr(m_username,5,(LENGTH(m_username)-8)) 
        || '_' || substr(m_version,0,6);
    
    t_obs_fact := 'OBFACT_' || m_suffix; 
    --t_obs_fact := substr(t_obs_fact,0,30);
    t_obs_dtype := REPLACE(t_obs_fact, 'OBFACT', 'ODTYPE');
    
    --t_obs_dtype := substr(t_obs_dtype, 0, 30);
    --DBMS_OUTPUT.PUT_LINE('tables: ' || t_obs_dtype || ' ' ||  t_obs_fact);
    
    HSSC_ETL.DROP_TABLE_IF_EXIST(t_obs_dtype, 'HSSC_ETL');
    HSSC_ETL.DROP_TABLE_IF_EXIST(t_obs_fact, 'HSSC_ETL');
    
    -- query to extract counts from 'observation_fact' table
    t_query := 'create table ' || t_obs_fact || ' nologging
    as (
      select  concept_cd,
              count(1) total_recs,
              count(distinct patient_num) pat_counts,
              count(distinct encounter_num) enc_counts
              from ' || m_username || '.observation_fact
              group by concept_cd
       )';
    
    --DBMS_OUTPUT.PUT_LINE(t_query); 
    
    -- execute to get counts into each table
    execute immediate t_query;
 
    -- outer join on concept dimension
    t_query := 'create table ' || t_obs_dtype || ' nologging
    as (
      select ob.CONCEPT_CD temp_concept_cd,
             cdim.concept_cd cdim_concept_cd,
             ob.total_recs total_obs,
             ob.pat_counts pat_counts,
             ob.enc_counts enc_counts,
             cdim.dtype dtype_from_concept_dim,
             cdim.npaths
             from ' || t_obs_fact || ' ob
    
      full outer join
        (
           select concept_cd,
                  substr(concept_path,2,instr(concept_path,''\'',2)-2) dtype,
                  count(1) npaths
                  from ' || m_username || '.concept_dimension dim
                  group by concept_cd,substr(concept_path,2,instr(concept_path,''\'',2)-2)
        ) cdim
     on(cdim.concept_cd = ob.concept_cd)
)';   

    --DBMS_OUTPUT.PUT_LINE(t_query);
    -- execute to get counts into each table
    execute immediate t_query;
    
    t_summary := REPLACE(t_obs_fact, 'OBFACT', 'SUMMARY');
    HSSC_ETL.DROP_TABLE_IF_EXIST(t_summary, 'HSSC_ETL');
    
    t_query := 'create table ' || t_summary || ' nologging
      as ( select
              substr(temp_concept_cd, 1, 
              instr(temp_concept_cd, '':'')-1) data_cdsys,
              substr(cdim_concept_cd, 1, 
                instr(temp_concept_cd, '':'')-1) cdim_cdsys,
              DTYPE_FROM_CONCEPT_DIM cdim_ont_category,
              count(1) n_uniq_cds, sum(total_obs) n_obs, 
              sum(enc_counts) n_enc,
              sum(pat_counts) n_pat from HSSC_ETL.' || t_obs_dtype || 
                ' group by substr(temp_concept_cd, 1, 
                    instr(temp_concept_cd, '':'')-1),
                substr(cdim_concept_cd, 1, 
                  instr(temp_concept_cd, '':'')-1),
              DTYPE_FROM_CONCEPT_DIM 
          )' ;
    
    --DBMS_OUTPUT.PUT_LINE(t_query);
    execute immediate t_query;
    
    -- todo (serialize and output into csv?)
    
  END LOOP;
 
  -- close cursor
  CLOSE m_cur;
 
END i2b2_aggregates;

-- end procedures

