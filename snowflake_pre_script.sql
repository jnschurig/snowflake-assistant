-------------------------------
-- Tagging Assistant Prereqs --
-------------------------------

-- Last updated with 1.1.0 release
-- Please re-run script if using version created prior to this release.

use role securityadmin;

-- Taskadmin role for managing tasks according to best practice: 
-- https://docs.snowflake.com/en/user-guide/tasks-intro.html#creating-a-task-administrator-role
create role if not exists taskadmin;
grant role taskadmin to role accountadmin;

use role accountadmin;

-- We will use sysadmin to do most of our work, so it will need read-access to the snowflake db.
grant imported privileges on database snowflake to role sysadmin;

-- Account-level task permissions
grant execute task, execute managed task on account to role taskadmin;

use role sysadmin;

-- Database objects --
create database if not exists tagging_assist_db;
grant usage on database tagging_assist_db to role taskadmin;

use database tagging_assist_db;

create schema if not exists scheduling;
grant usage, create task on schema scheduling to role taskadmin;

create schema if not exists utility;
grant usage, create procedure on schema utility to role taskadmin;

create schema if not exists tagging;
use schema tagging;

create tag if not exists tag_assistant_enabled
  allowed_values 'y', 'n'
  comment = 'Tracking whether the tag assistant is enabled on a given warehouse'
;

create schema if not exists metadata;
use schema metadata;

create or replace view warehouse_applied_tags copy grants as (
with 
  base_tags as ( -- Aggregate applied tags into variant object
    select object_name as warehouse_name
          ,object_agg(tag_name, tag_value::variant) as tag_assignments
      from snowflake.account_usage.tag_references
     where object_deleted is null
       and tag_database = 'TAGGING_ASSIST_DB'
       and tag_schema = 'TAGGING'
       and domain = 'WAREHOUSE'
     group by object_name
    )
    select warehouse_name
          ,tag_assignments:"TAG_ASSISTANT_ENABLED"::string as assistant_enabled
          ,object_delete(tag_assignments, 'TAG_ASSISTANT_ENABLED') as tag_assignments
      from base_tags
)
;

create or replace view warehouse_usage_last_month copy grants as (
with
  get_usage as ( -- Aggregate warehouse account usage and filter by most recent 30 days
    select warehouse_name
          ,credits_used
          ,start_time
          ,end_time
      from snowflake.account_usage.warehouse_metering_history
     where start_time >= dateadd('day', -30, current_date)
    )
-- Combine tags with warehouse usage
select nvl(a.warehouse_name, b.warehouse_name) as warehouse_name
      ,nvl(b.assistant_enabled, 'n') as assistant_enabled
      ,nvl(b.tag_assignments, object_construct()) as tag_assignments
      ,nvl(a.credits_used, 0) as credits_used
      ,a.start_time
      ,a.end_time
      ,to_date(a.start_time) as start_date
      ,dayofweek(start_date) || ' ' || dayname(start_date) as start_day_name
      ,to_char(a.start_time, 'hh24') as start_hour
  from get_usage a
  full join warehouse_applied_tags b on a.warehouse_name = b.warehouse_name
)
;

-- Warehouses --
create warehouse if not exists tagging_assist_scheduler_wh
  warehouse_size = xsmall
  auto_suspend = 1
  auto_resume = true
  initially_suspended = true
  comment = 'Warehouse used for warehouse re-sizing tasks'
  with tag (tagging_assist_db.tagging.tag_assistant_enabled = 'y')
;

grant usage on warehouse tagging_assist_scheduler_wh to role taskadmin;

create warehouse if not exists tagging_assist_wh
  warehouse_size = xsmall
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'Main warehouse for use with the Tagging Assistant app.'
  with tag (tagging_assist_db.tagging.tag_assistant_enabled = 'y')
;

-- In case the warehouse already exists...
alter warehouse tagging_assist_wh set warehouse_size = xsmall;


use role taskadmin;
use schema tagging_assist_db.utility;

create or replace procedure sp_create_warehouse_size_task(WAREHOUSE_NAME varchar, WAREHOUSE_SIZE varchar, CRON_STRING varchar, SCHEDULE_INDEX varchar, TIMEZONE_STR varchar)
returns variant
language javascript
--called on null input
comment = 'Procedure for creating and enabling tasks for resizing warehouses.'
execute as owner
as 
$$
// Initialize Variables
var result = {};

// Function to easily execute and return first-record, single-column results.
function run_sql(script_text, return_column) {
	var statement = snowflake.createStatement({sqlText: script_text});
	var fun_is_ok = true;
	var fun_result = {};
	
	try {
		var stmt_result = statement.execute();
	}
	catch(err) {
		err.statement = script_text
		fun_result = err;
		fun_is_ok = false;
		fun_result.result = "ERROR";
	}
	if(fun_is_ok) {
		stmt_result.next();
		fun_result.result = stmt_result.getColumnValue(return_column);
		//fun_result = "OK";
	}
	return fun_result;
}

// Assemble the sql
var task_name = "scheduling.resize_" + WAREHOUSE_NAME.toLowerCase() + "_schedule_" + SCHEDULE_INDEX;

var create_sql = `-- Create task
create or replace task ` + task_name + `
  warehouse = tagging_assist_scheduler_wh
  schedule = 'USING CRON ` + CRON_STRING + ` ` + TIMEZONE_STR + `' 
as 
alter warehouse ` + WAREHOUSE_NAME.toLowerCase() + ` set warehouse_size = ` + WAREHOUSE_SIZE + `
`;

var resume_sql = "alter task " + task_name + " resume";

var grant_sql = "grant all on task " + task_name + " to role sysadmin";

// Run the sql
result.create = run_sql(create_sql, 1);

result.resume = run_sql(resume_sql, 1);

result.grant = run_sql(grant_sql, 1);

return result;
$$
;

grant usage on procedure sp_create_warehouse_size_task(varchar, varchar, varchar, varchar, varchar) to role sysadmin;

create or replace procedure sp_drop_warehouse_size_task(TASK_NAME varchar)
returns variant
language javascript
--called on null input
comment = 'Procedure for dropping tasks for resizing warehouses.'
execute as owner
as 
$$
// Initialize Variables
var result = {};

var drop_sql = "drop task scheduling." + TASK_NAME;

var drop_stmt = snowflake.createStatement({sqlText: drop_sql});

try {
	var drop_result = drop_stmt.execute();
	drop_result.next();
}
catch(err) {
	result.drop = err;
	result.statement = drop_sql;
	return result;
}

result.drop = drop_result.getColumnValue(1);
return result;
$$
;

grant usage on procedure sp_drop_warehouse_size_task(varchar) to role sysadmin;

create or replace procedure sp_pause_resume_warehouse_size_task(TASK_NAME varchar, ACTION varchar)
returns variant
language javascript
--called on null input
comment = 'Procedure for toggling (suspend or resume) tasks for resizing warehouses.'
execute as owner
as 
$$
// Initialize Variables
var result = {};

if(['suspend', 'resume'].includes(ACTION)) {

	var alter_sql = "alter task scheduling." + TASK_NAME + " " + ACTION;

	var alter_stmt = snowflake.createStatement({sqlText: alter_sql});

	try {
		var alter_result = alter_stmt.execute();
		alter_result.next();
	}
	catch(err) {
		result.alter = err;
		result.statement = alter_sql;
		return result;
	}

	result.alter = alter_result.getColumnValue(1);
}
else result.alter = "Action: " + ACTION + " not supported";
return result;
$$
;

grant usage on procedure sp_pause_resume_warehouse_size_task(varchar, varchar) to role sysadmin;
