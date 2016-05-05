create or replace PROCEDURE          patdim_counts(m_version IN VARCHAR
DEFAULT TO_CHAR(SYSDATE, 'YYYYMMDD')) authid current_user
AS
    -- array of varchar2 type
    TYPE VARRAY_VARCHAR IS VARYING ARRAY(30) OF VARCHAR2(30);
    
    m_table VARCHAR2(60);
    m_query VARCHAR(5000);
    t_query VARCHAR(5000);
    
    m_pat_attrib VARRAY_VARCHAR;
    m_pat_value VARRAY_VARCHAR;
    
    -- cursor to fetch i2b2*data instances
    m_cur SYS_REFCURSOR;

    -- username for i2b2 instance
    m_data varchar(100);
    m_meta varchar(100);

    -- suffix reflects table version
    m_suffix varchar(100);
    
BEGIN

    m_pat_attrib := VARRAY_VARCHAR( 'Age','Ethnicity', 
    'Urban-Rural Classification', 'Vital Status', 'Military Status', 
    'Sex', 'Race' );

    m_pat_value := VARRAY_VARCHAR( 'AGE_IN_YEARS_NUM', 'ETHNICITY_CD',
    'URBAN_RURAL_CD', 'VITAL_STATUS_CD', 'MILITARY_STATUS_CD',
    'SEX_CD', 'RACE_CD');
    
    -- get distinct usernames from dba_users table matching 'I2B2*DATA'
    m_query:='select distinct(username) from dba_users where username like ''I2B2%META''';
 
    -- open cursor and loop over instances (usernames)
    OPEN m_cur FOR m_query;
    LOOP
 
      FETCH m_cur INTO m_meta;
      EXIT WHEN m_cur%NOTFOUND;
    
      -- prompt current user being processed
      DBMS_OUTPUT.PUT_LINE('Patient dimension counts for ' || m_meta);   

      m_suffix := substr(m_meta,5,(LENGTH(m_meta)-8))
        || '_' || substr(m_version,0,6);
      
      
      m_table := 'PATDIM_' || m_suffix;
      --DBMS_OUTPUT.PUT_LINE(m_table);
      
      HSSC_ETL.DROP_TABLE_IF_EXIST(m_table, 'HSSC_ETL');
      
      m_query := 'create table ' || m_table || ' (
        attr_name varchar(200) not null, attr_value varchar(300), 
        c_name varchar(500), c_fullname varchar(1000), 
        attrcount number, patcount number)';  
      
      execute immediate m_query;
      
      m_data := REPLACE(m_meta, 'META', 'DATA');
      
      FOR it IN 1 .. m_pat_attrib.count LOOP
        DBMS_OUTPUT.PUT_LINE('Attribute : ' || m_pat_attrib(it) );
        
        t_query := 'INSERT INTO ' || m_table || ' (SELECT  ''' || 
            m_pat_attrib(it) || ''' attr, a.' || m_pat_value(it) 
            || ' value, ov.c_name, ov.c_fullname, a.n_attrs attr_count, 
            a.n_recs pat_count from 
          ( select * from ' || m_meta || '.ONT_DEMO where c_basecode
          like ''' || m_pat_attrib(it) || ':%'' ) ov full outer join 
            ( select ' || m_pat_value(it) || ', 
            count(' || m_pat_value(it) || ') n_attrs, count(1) n_recs from
              ' || m_data || '.PATIENT_DIMENSION group by ' 
                || m_pat_value(it) || ') a on 
                   (ov.c_dimcode = to_char(a.' || m_pat_value(it) || ')))';
         DBMS_OUTPUT.PUT_LINE(t_query); 
         execute immediate t_query;
      END LOOP;  

  END LOOP;
 
  -- close cursor
  CLOSE m_cur;
  
END patdim_counts;
