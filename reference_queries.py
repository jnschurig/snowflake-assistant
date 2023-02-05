

WAREHOUSE_QUERY_HISTORY_SUMMARY = '''-- Summarize warehouse usage over timeframe
select nvl(warehouse_name, 'None') as warehouse_name
      ,listagg(distinct warehouse_size, ', ') as size_in_use
      ,count(0) as total_queries
      ,count(distinct md5(query_text)) as total_distinct_queries
      ,count(CASE when execution_status = 'ERROR' then 1 else null END) as error_count
      ,sum(total_elapsed_time) as total_query_time
      ,sum(execution_time) as total_execution_time
      ,sum(compilation_time) as total_compilation_time
      ,sum(queued_provisioning_time) as total_queued_provisioning_time
      ,sum(queued_repair_time) as total_queued_repair_time
      ,sum(queued_overload_time) as total_queued_overload_time
      ,sum(transaction_blocked_time) as total_transaction_blocked_time
      ,avg(total_elapsed_time) as average_query_time
      ,avg(execution_time) as average_execution_time
      ,avg(compilation_time) as average_compilation_time
      ,avg(queued_provisioning_time) as average_queued_provisioning_time
      ,avg(queued_repair_time) as average_queued_repair_time
      ,avg(queued_overload_time) as average_queued_overload_time
      ,avg(transaction_blocked_time) as average_transaction_blocked_time
      ,avg(div0(partitions_scanned, partitions_total)) as average_pct_of_partitions_scanned
      ,sum(bytes_scanned) as total_bytes_scanned
      ,sum(bytes_written) as total_bytes_written
      ,sum(bytes_spilled_to_local_storage) as total_bytes_spilled_to_local_storage
      ,sum(bytes_spilled_to_remote_storage) as total_bytes_spilled_to_remote_storage
      ,avg(bytes_scanned) as average_bytes_scanned
      ,avg(bytes_written) as average_bytes_written
      ,avg(bytes_spilled_to_local_storage) as average_bytes_spilled_to_local_storage
      ,avg(bytes_spilled_to_remote_storage) as average_bytes_spilled_to_remote_storage
  from snowflake.account_usage.query_history
 where start_time >= dateadd('day', -{date_range}, current_date)
 group by 1
'''

WAREHOUSE_TAG_SUMMARY = '''-- Show all tags for a given warehouse
select object_name as warehouse_name
      ,array_agg(object_construct('tag_db', tag_database
                       ,'tag_schema', tag_schema
                       ,'tag_name', tag_name
                       ,'value', tag_value
                       )) as tag_json
  from snowflake.account_usage.tag_references
 where domain = 'WAREHOUSE'
 group by object_name 
'''

WAREHOUSE_METERING_SUMMARY = '''-- Summarize WH usage and show daily averages over the period
with 
  daily_summary as (
    select warehouse_name
          ,date_trunc('day', start_time) as metering_day
          ,sum(credits_used) as credits_used 
          ,sum(credits_used_compute) as credits_used_compute
          ,sum(credits_used_cloud_services) as credits_used_cloud_services
      from snowflake.account_usage.warehouse_metering_history 
     where start_time >= dateadd('day', -{date_range}, current_date)
     group by warehouse_name, metering_day
    )
select warehouse_name 
      ,avg(credits_used) as daily_avg_credits_used 
      ,avg(credits_used_compute) as daily_avg_credits_used_compute
      ,avg(credits_used_cloud_services) as daily_avg_credits_used_cloud_services
      ,sum(credits_used) as credits_used 
      ,sum(credits_used_compute) as credits_used_compute
      ,sum(credits_used_cloud_services) as credits_used_cloud_services
  from daily_summary
 group by warehouse_name
'''