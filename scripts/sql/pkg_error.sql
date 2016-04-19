/* Create Error Log table to capture any error from execution of something
*/
CREATE TABLE ERRORLOG
  (
    ID NUMBER NOT NULL,
    TIME DATE NOT NULL,
    CODE           VARCHAR2(9 BYTE) NOT NULL,
    MESSAGE        VARCHAR2(2000 BYTE) NOT NULL,
    PACKAGE_NAME   VARCHAR2(100 BYTE),
    PROCEDURE_NAME VARCHAR2(100 BYTE) NOT NULL,
    LOCATION       NUMBER,
    PARAMETERS     VARCHAR2(4000 BYTE)
  )
/

CREATE SEQUENCE ERRORLOG_ID_SEQ
  MINVALUE 1 MAXVALUE 999999999999999999999999999
  INCREMENT BY 1 START WITH 1
  CACHE 20 NOORDER NOCYCLE ;
/

CREATE OR REPLACE TRIGGER T_ERRORLOG_BI
BEFORE INSERT
ON ERRORLOG
REFERENCING OLD AS OLD NEW AS NEW
FOR EACH ROW
begin
 if :new.id is null then
  select errorlog_id_seq.nextval into :new.id from dual;
 end if;
end T_ERRORLOG_BI;
/

ALTER TABLE ERRORLOG ADD (PRIMARY KEY (ID));
/

CREATE OR REPLACE
PACKAGE pkg_error
IS
PROCEDURE log(
    p_error_code errorlog.code%type,
    p_error_message errorlog.message%type,
    p_package errorlog.package_name%type DEFAULT NULL,
    p_procedure errorlog.procedure_name%type,
    p_location errorlog.location%type DEFAULT NULL,
    p_parameters errorlog.parameters%type DEFAULT NULL);
END pkg_error;
/

CREATE OR REPLACE
PACKAGE body pkg_error
IS
PROCEDURE log(
    p_error_code errorlog.code%type,
    p_error_message errorlog.message%type,
    p_package errorlog.package_name%type DEFAULT NULL,
    p_procedure errorlog.procedure_name%type,
    p_location errorlog.location%type DEFAULT NULL,
    p_parameters errorlog.parameters%type DEFAULT NULL)
IS
  pragma autonomous_transaction;
BEGIN
  INSERT
  INTO errorlog
    (
      TIME,
      code,
      MESSAGE,
      package_name,
      procedure_name,
      location,
      parameters
    )
    VALUES
    (
      sysdate,
      p_error_code,
      p_error_message,
      p_package,
      p_procedure,
      p_location,
      p_parameters
    );
  COMMIT;
END log;
END pkg_error;
/
