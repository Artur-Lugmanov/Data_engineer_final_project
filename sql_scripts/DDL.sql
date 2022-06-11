--Подготовка структур--
CREATE TABLE de3at.lugm_stg_transactions (
    trans_id     VARCHAR2(15),
    table_date   DATE,
    card_num     VARCHAR2(19),
    oper_type    VARCHAR2(10),
    amt          NUMBER(10, 2),
    oper_result  VARCHAR2(10),
    terminal     VARCHAR2(8)
);

CREATE TABLE de3at.lugm_stg_passport_blacklist (
    passport_num   VARCHAR2(11),
    entry_dt       DATE
);

CREATE TABLE de3at.lugm_stg_terminals (
    terminal_id         VARCHAR2(10),
    terminal_type       VARCHAR2(5),
    terminal_city       VARCHAR2(50),
    terminal_address     VARCHAR2(100),
    update_dt           DATE
);

CREATE TABLE de3at.lugm_stg_cards AS 
SELECT card_num, account, update_dt
FROM bank.cards
WHERE 1 = 0;

CREATE TABLE de3at.lugm_stg_accounts AS 
SELECT account, valid_to, client, update_dt
FROM bank.accounts
WHERE 1 = 0;

CREATE TABLE de3at.lugm_stg_clients AS 
SELECT *
FROM bank.clients
WHERE 1 = 0;
ALTER TABLE de3at.lugm_stg_clients
DROP COLUMN create_dt;

CREATE TABLE de3at.lugm_stg_terminals_del AS 
SELECT terminal_id FROM de3at.lugm_stg_terminals
WHERE 1 = 0;

CREATE TABLE de3at.lugm_stg_cards_del AS 
SELECT card_num FROM de3at.lugm_stg_cards
WHERE 1 = 0;

CREATE TABLE de3at.lugm_stg_accounts_del AS 
SELECT account FROM de3at.lugm_stg_accounts
WHERE 1 = 0;

CREATE TABLE de3at.lugm_stg_clients_del AS 
SELECT client_id FROM de3at.lugm_stg_clients
WHERE 1 = 0;

CREATE TABLE de3at.lugm_dwh_fact_transactions AS 
SELECT * FROM de3at.lugm_stg_transactions
WHERE 1 = 0;

CREATE TABLE de3at.lugm_dwh_fact_pssprt_blcklst AS 
SELECT * FROM de3at.lugm_stg_pssprt_blcklst
WHERE 1 = 0;

CREATE TABLE de3at.lugm_dwh_dim_clients_his AS 
SELECT * FROM de3at.lugm_stg_clients
WHERE 1 = 0;
ALTER TABLE de3at.lugm_dwh_dim_clients_his ADD ( 
effective_from DATE,
effective_to DATE,
deleted_flg CHAR (1));
ALTER TABLE de3at.lugm_dwh_dim_clients_his 
DROP COLUMN update_dt;

CREATE TABLE de3at.lugm_dwh_dim_terminals_his AS 
SELECT 
* FROM de3at.lugm_stg_terminals
WHERE 1 = 0;
ALTER TABLE de3at.lugm_dwh_dim_terminals_his ADD ( 
effective_from DATE,
effective_to DATE,
deleted_flg CHAR (1));
ALTER TABLE de3at.lugm_dwh_dim_terminals_his 
DROP COLUMN update_dt;


CREATE TABLE de3at.lugm_dwh_dim_cards_his AS 
SELECT * FROM de3at.lugm_stg_cards
WHERE 1 = 0;
ALTER TABLE de3at.lugm_dwh_dim_cards_his ADD ( 
effective_from DATE,
effective_to DATE,
deleted_flg CHAR (1));
ALTER TABLE de3at.lugm_dwh_dim_cards_his 
DROP COLUMN update_dt;

CREATE TABLE de3at.lugm_dwh_dim_accounts_his AS 
SELECT * FROM de3at.lugm_stg_accounts
WHERE 1 = 0;
ALTER TABLE de3at.lugm_dwh_dim_accounts_his ADD ( 
effective_from DATE,
effective_to DATE,
deleted_flg CHAR (1));
ALTER TABLE de3at.lugm_dwh_dim_accounts_his 
DROP COLUMN update_dt;

CREATE TABLE de3at.lugm_dwh_fact_transactions AS 
SELECT * FROM de3at.lugm_stg_transactions;

CREATE TABLE de3at.lugm_dwh_fact_pssprt_blcklst AS 
SELECT * FROM de3at.lugm_stg_pssprt_blcklst;

create table de3at.lugm_meta(
    schema_name VARCHAR2(30),
    table_name VARCHAR2(30),
    last_update_dt DATE)

INSERT ALL
into de3at.lugm_meta( schema_name, table_name, last_update_dt )
values ( 'de3at', 'terminals', to_date('01.01.1890 00:00:00','DD.MM.YYYY HH24:MI:SS'))
into de3at.lugm_meta( schema_name, table_name, last_update_dt )
values( 'de3at', 'cards', to_date('01.01.1890 00:00:00','DD.MM.YYYY HH24:MI:SS'))
into de3at.lugm_meta( schema_name, table_name, last_update_dt )
values( 'de3at', 'accounts', to_date('01.01.1890 00:00:00','DD.MM.YYYY HH24:MI:SS'))
into de3at.lugm_meta( schema_name, table_name, last_update_dt )
values( 'de3at', 'clients', to_date('01.01.1890 00:00:00','DD.MM.YYYY HH24:MI:SS'))
SELECT * FROM DUAL;

CREATE TABLE de3at.lugm_rep_fraud (
    event_dt            DATE,
    passport_num        VARCHAR2(15),
    fio                 VARCHAR2(150),
    phone               VARCHAR2(20),
    event_type          VARCHAR2(50),
    report_dt           DATE
);

create table de3at.lugm_meta_rep (
    schema_name VARCHAR2(30),
    table_name VARCHAR2(30),
    last_update_dt DATE);

insert into de3at.lugm_meta_rep( schema_name, table_name, last_update_dt )
values ( 'de3at', 'rep_fraud', to_date('01.01.1890 00:00:00','DD.MM.YYYY HH24:MI:SS'))