/*
 * After creating HSSC_ETL user, log in and create the private MPI DB Link:
 
 CREATE DATABASE LINK "MPI"
 CONNECT TO "PATIENT" IDENTIFIED BY <password>
 USING 'hssc-cdw-mpidb-d:1521/mpidev'
 ;
 
 TODO: Orchestrate creation of user with grants and creation of private DB
       link, which requires directly connecting as the new user.
 */
 --drop user HSSC_ETL cascade;
create user HSSC_ETL identified by changeme;

grant CREATE SESSION to HSSC_ETL;
grant UNLIMITED TABLESPACE to HSSC_ETL;
grant SELECT ANY TABLE to HSSC_ETL;
grant CREATE DATABASE LINK to HSSC_ETL;

BEGIN
  FOR x IN (SELECT * FROM dba_tables WHERE owner in ('CDW'))
  LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT, INSERT, UPDATE, DELETE ON '
                       || x.owner || '.' || x.table_name
                       || ' TO HSSC_ETL';
  END LOOP;
END;
