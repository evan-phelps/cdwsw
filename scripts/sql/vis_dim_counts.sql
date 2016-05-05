create or replace PROCEDURE           visdim_counts( m_version IN VARCHAR
DEFAULT TO_CHAR(SYSDATE, 'YYYYMMDD'))
authid current_user
AS
    -- array of varchar2 type
    TYPE VARRAY_VARCHAR IS VARYING ARRAY(30) OF VARCHAR2(30);
    
    m_table VARCHAR2(60);
    m_query VARCHAR(5000);
    t_query VARCHAR(5000);
    
    m_vis_attrib VARRAY_VARCHAR;
    m_vis_value VARRAY_VARCHAR;
    
    -- cursor to fetch i2b2*data instances
    m_cur SYS_REFCURSOR;

    -- username for i2b2 instance
    m_data varchar(100);
    m_meta varchar(100);

    -- suffix reflects table version
    m_suffix varchar(100);
    
BEGIN


    m_vis_attrib := VARRAY_VARCHAR( 'Accommodation','Admission Source', 
    'Admission Type', 'Discharge Disposition', 'Encounter SubType', 
    'Encounter Type', 'Financial Class', 'Hospital Service' );

    m_vis_value := VARRAY_VARCHAR( 'accomm_cd', 'admission_source',
    'admission_type', 'disch_disp', 'type_cd',
    'inout_cd', 'fin_class_group', 'hosp_srv_group');
    
    -- get distinct usernames from dba_users table matching 'I2B2*DATA'
    m_query:='select distinct(username) from dba_users where username like ''I2B2%META''';
 
    -- open cursor and loop over instances (usernames)
    OPEN m_cur FOR m_query;
    LOOP
 
      FETCH m_cur INTO m_meta;
      EXIT WHEN m_cur%NOTFOUND;
    
      -- prompt current user being processed
      DBMS_OUTPUT.PUT_LINE('Visit dimension counts for ' || m_meta);   
      
      m_suffix := substr(m_meta,5,(LENGTH(m_meta)-8))
        || '_' || substr(m_version,0,6);
      
      
      m_table := 'VISDIM_' || m_suffix;
      --DBMS_OUTPUT.PUT_LINE(m_table);
      
      HSSC_ETL.DROP_TABLE_IF_EXIST(m_table, 'HSSC_ETL');

      m_query := 'create table ' || m_table || ' (
          attr_name varchar(200) not null, attr_value varchar(300), 
          c_name varchar(500), c_fullname varchar(1000), 
          attrcount number, viscount number, patcount number)';  
    
      execute immediate m_query;
      --DBMS_OUTPUT.PUT_LINE(m_query);
      
      m_data := REPLACE(m_meta, 'META', 'DATA');
      
      FOR it IN 1 .. m_vis_attrib.count LOOP
        DBMS_OUTPUT.PUT_LINE('Attribute : ' || m_vis_attrib(it) );
        
        t_query := 'INSERT INTO ' || m_table || ' (SELECT  ''' || 
            m_vis_attrib(it) || ''' attr, v.' || m_vis_value(it) 
            || ' value, ov.c_name, ov.c_fullname, v.n_attrs attr_count,
            v.n_recs visit_count, v.precs pat_count from 
          ( select * from ' || m_meta || '.ONT_VISIT where c_basecode
          like ''' || m_vis_attrib(it) || ':%'' ) ov full outer join 
            ( select ' || m_vis_value(it) || ', count('
            || m_vis_value(it) || ') n_attrs, count(distinct patient_num) precs, 
            count(1) n_recs from ' || m_data || '.VISIT_DIMENSION group by ' 
                || m_vis_value(it) || ') v on 
                   (ov.c_dimcode = v.' || m_vis_value(it) || '))';
         DBMS_OUTPUT.PUT_LINE(t_query); 
         execute immediate t_query;
      END LOOP;  

  END LOOP;
 
  -- close cursor
  CLOSE m_cur;
  
END visdim_counts;
