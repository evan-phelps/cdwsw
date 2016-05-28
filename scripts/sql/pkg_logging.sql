DROP TABLE INFOLOG;
CREATE TABLE INFOLOG
  (
    ID NUMBER NOT NULL,
    LOG_LEVEL VARCHAR2(5 BYTE) NOT NULL,
    TIME DATE NOT NULL,
    CODE           VARCHAR2(9 BYTE),
    MESSAGE        VARCHAR2(2000 BYTE) NOT NULL,
    PACKAGE_NAME   VARCHAR2(100 BYTE),
    PROCEDURE_NAME VARCHAR2(100 BYTE) NOT NULL,
    LOCATION       NUMBER,
    PARAMETERS     VARCHAR2(4000 BYTE)
  )
/

DROP SEQUENCE INFOLOG_ID_SEQ;
CREATE SEQUENCE INFOLOG_ID_SEQ
  MINVALUE 1 MAXVALUE 999999999999999999999999999
  INCREMENT BY 1 START WITH 1
  CACHE 20 NOORDER NOCYCLE ;
/

CREATE OR REPLACE TRIGGER T_INFOLOG_BI
BEFORE INSERT
ON INFOLOG
REFERENCING OLD AS OLD NEW AS NEW
FOR EACH ROW
begin
 if :new.id is null then
  select infolog_id_seq.nextval into :new.id from dual;
 end if;
end T_INFOLOG_BI;
/

ALTER TABLE INFOLOG ADD (PRIMARY KEY (ID));
/

CREATE OR REPLACE
PACKAGE pkg_logging
IS
PROCEDURE log(
    p_log_level infolog.log_level%type DEFAULT 'INFO',
    p_code infolog.code%type DEFAULT NULL,
    p_message infolog.message%type,
    p_package infolog.package_name%type DEFAULT NULL,
    p_procedure infolog.procedure_name%type,
    p_location infolog.location%type DEFAULT NULL,
    p_parameters infolog.parameters%type DEFAULT NULL);
PROCEDURE error(
    p_log_level infolog.log_level%type DEFAULT 'ERROR',
    p_code infolog.code%type DEFAULT NULL,
    p_message infolog.message%type,
    p_package infolog.package_name%type DEFAULT NULL,
    p_procedure infolog.procedure_name%type,
    p_location infolog.location%type DEFAULT NULL,
    p_parameters infolog.parameters%type DEFAULT NULL);
END pkg_logging;
/

CREATE OR REPLACE
PACKAGE body pkg_logging
IS
PROCEDURE log(
    p_log_level infolog.log_level%type DEFAULT 'INFO',
    p_code infolog.code%type DEFAULT NULL,
    p_message infolog.message%type,
    p_package infolog.package_name%type DEFAULT NULL,
    p_procedure infolog.procedure_name%type,
    p_location infolog.location%type DEFAULT NULL,
    p_parameters infolog.parameters%type DEFAULT NULL)
IS
  pragma autonomous_transaction;
BEGIN
  INSERT
  INTO infolog
    (
      TIME,
      log_level,
      code,
      message,
      package_name,
      procedure_name,
      location,
      parameters
    )
    VALUES
    (
      sysdate,
      p_log_level,
      p_code,
      p_message,
      p_package,
      p_procedure,
      p_location,
      p_parameters
    );
  COMMIT;
END log;

PROCEDURE error(
    p_log_level infolog.log_level%type DEFAULT 'ERROR',
    p_code infolog.code%type,
    p_message infolog.message%type,
    p_package infolog.package_name%type DEFAULT NULL,
    p_procedure infolog.procedure_name%type,
    p_location infolog.location%type DEFAULT NULL,
    p_parameters infolog.parameters%type DEFAULT NULL)
IS
  pragma autonomous_transaction;
BEGIN
  log(p_log_level, p_code, p_message,
      p_package, p_procedure, p_location, p_parameters);
END error;

END pkg_logging;
/
