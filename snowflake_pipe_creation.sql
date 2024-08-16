--change the role to accountadmin
USE ROLE ACCOUNTADMIN;

create or replace warehouse capstone_wh;
use warehouse capstone_wh;
 
create database capstone_db;
create database PC_DBT_DB;
 
use database capstone_db;
use database PC_DBT_DB;	
 
--drop database PC_DBT_DB;
--drop database capstone_db;
--File_format
create or replace file format csv_file_format
type='csv'
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1;
 
--CREATE STORAGE INTEGRATION(CREATE ROLE THROUGH IAM WITH S3 ALL ACCESS AND COPY ARM FROM ROLE)
create or replace storage integration capstone_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::891377129521:role/capstone'
  storage_allowed_locations = ('s3://capstonesandhya/capstone_folder/');
 
  ----describe the integration created(copy the 5,6 arn and id in trust relations in roles created)
  desc integration capstone_int;
 
 
  --Create external stage object
  create or replace stage capstone_stage
  URL = 's3://capstonesandhya/capstone_folder/'
  STORAGE_INTEGRATION = capstone_int
  file_format = csv_file_format;

 list @capstone_stage;
 
--Create tables for data ingestion
 
--Transaction table
create or replace table transaction(
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);
 
--Customer Table
create or replace table customer(
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);
 
--Accounts Table
create or replace table account(
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);
 
--Credit data table
create or replace table credit(
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

drop table account;
drop table credit;
drop table customer;
drop table transaction;
 
-- Create a Snowpipe to load data from S3 into the list_table
CREATE OR REPLACE PIPE transaction_pipe AUTO_INGEST = TRUE AS
COPY INTO transaction 
FROM @capstone_stage/transactions.csv
FILE_FORMAT = csv_file_format;
--VALIDATION_MODE = RETURN_ALL_ERRORS;
 
CREATE OR REPLACE PIPE customer_pipe AUTO_INGEST = TRUE AS
COPY INTO customer
FROM @capstone_stage/customers.csv
FILE_FORMAT = csv_file_format;
 
CREATE OR REPLACE PIPE account_pipe AUTO_INGEST = TRUE AS
COPY INTO account 
FROM @capstone_stage/accounts.csv
FILE_FORMAT = csv_file_format;
 
CREATE OR REPLACE PIPE credit_pipe AUTO_INGEST = TRUE AS
COPY INTO credit 
FROM @capstone_stage/credit_data.csv
FILE_FORMAT = csv_file_format;
 
show pipes;
 
alter pipe transaction_pipe refresh;
alter pipe customer_pipe refresh;
alter pipe account_pipe refresh;
alter pipe credit_pipe refresh;
 
Select  SYSTEM$PIPE_STATUS('transaction_pipe');
Select  SYSTEM$PIPE_STATUS('customer_pipe');
Select  SYSTEM$PIPE_STATUS('account_pipe');
Select  SYSTEM$PIPE_STATUS('credit_pipe');
 
select * from transaction;
select * from customer;
select * from account;
select * from credit;
 
 
create role PC_DBT_ROLE;
GRANT All PRIVILEGES ON DATABASE PC_DBT_DB TO ROLE pc_dbt_role;
CREATE SCHEMA TEST_SCHEMA;
GRANT All PRIVILEGES on SCHEMA TEST_SCHEMA TO ROLE pc_dbt_role;
 
create table t1(
c1 int,
c2 int
);
 
insert into t1 values(200, 400);
select * from PC_DBT_DB.test_schema.T1;
 
REVOKE APPLYBUDGET ON DATABASE PC_DBT_DB FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE PC_DBT_DB to role PC_DBT_ROLE;
grant all privileges on schema PUBLIC to role PC_DBT_ROLE;
grant select on all tables in schema PUBLIC to role PC_DBT_ROLE;
 
 
select * from customer;
