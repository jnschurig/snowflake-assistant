-- Snowflake Assistant DB/Schema Initialization Script
-- Object names chosen dynamically. If running locally, 
-- see constants.py for role and database names.

-- Typically securityadmin or useradmin
use role {security_object_role};

create role if not exists {snowflake_assistant_db}_role;
grant role {snowflake_assistant_db}_role to role {database_object_role};

-- Typically sysadmin
use role {database_object_role};

create database if not exists {snowflake_assistant_db};
grant usage on database {snowflake_assistant_db} to role {snowflake_assistant_db}_role;

create schema if not exists {snowflake_assistant_db}.{schema_name};
grant all on schema {snowflake_assistant_db}.{schema_name} to role {snowflake_assistant_db}_role;

create warehouse if not exists {snowflake_assistant_db}_wh
  warehouse_size = xsmall
  auto_resume = true 
  max_cluster_count = 1
  auto_suspend = 300
;
grant usage on warehouse {snowflake_assistant_db}_wh to role {snowflake_assistant_db}_role;

use role {snowflake_assistant_db}_role;

