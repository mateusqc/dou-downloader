ALTER SESSION SET CONTAINER = FREEPDB1;


CREATE USER DOU
IDENTIFIED BY dou
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE TEMP
QUOTA unlimited on users
QUOTA 0 on system;


GRANT CONNECT, RESOURCE TO DOU;


GRANT CREATE SESSION, CREATE VIEW, CREATE TABLE, ALTER SESSION, CREATE SEQUENCE, CREATE PROCEDURE, CREATE TRIGGER TO DOU;
GRANT CREATE SYNONYM, CREATE DATABASE LINK, UNLIMITED TABLESPACE TO DOU;