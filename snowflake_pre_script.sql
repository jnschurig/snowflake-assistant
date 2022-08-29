-------------------------------
-- Tagging Assistant Prereqs --
-------------------------------

use role accountadmin;

-- We will use sysadmin to do most of our work, so it will need read-access to the snowflake db.
grant imported privileges on database snowflake to role sysadmin;

use role sysadmin;

-- Database objects --
create database if not exists tagging_assist_db;
create schema if not exists tagging_assist_db.tagging;
create tag if not exists tagging_assist_db.tagging.tag_assistant_enabled
  allowed_values 'y', 'n'
  comment = 'Tracking whether the tag assistant is enabled on a given warehouse'
;

create schema if not exists tagging_assist_db.metadata;
create or replace view tagging_assist_db.metadata.warehouse_usage_last_month copy grants as (
with 
  base_tags as ( -- Aggregate applied tags into variant object
    select object_name as warehouse_name
          ,object_agg(tag_name, tag_value::variant) as tag_assignments
      from snowflake.account_usage.tag_references
     where tag_database = 'TAGGING_ASSIST_DB'
       and tag_schema = 'TAGGING'
       and domain = 'WAREHOUSE'
     group by object_name
    )
 ,flag_warehouses as ( -- Pull out the assistant enabled tag
    select warehouse_name
          ,tag_assignments:"TAG_ASSISTANT_ENABLED"::string as assistant_enabled
          ,object_delete(tag_assignments, 'TAG_ASSISTANT_ENABLED') as tag_assignments
      from base_tags
    )
 ,get_usage as ( -- Aggregate warehouse account usage and filter by most recent 30 days
    select warehouse_name
          ,sum(credits_used) as total_credits_used
      from snowflake.account_usage.warehouse_metering_history
     where start_time >= dateadd('day', -30, current_timestamp)
     group by warehouse_name
    )
-- Combine tags with warehouse usage
select nvl(a.warehouse_name, b.warehouse_name) as warehouse_name
      ,nvl(b.assistant_enabled, 'n') as assistant_enabled
      ,nvl(b.tag_assignments, object_construct()) as tag_assignments
      ,nvl(a.total_credits_used, 0) as total_credits_used
  from get_usage a
  full join flag_warehouses b on a.warehouse_name = b.warehouse_name
)

-- Warehouse --
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
