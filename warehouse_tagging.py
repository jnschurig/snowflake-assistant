import pandas as pd
import streamlit as st
import st_snowpark_session as sesh
import reference_queries as rq
import re
import constants
import utility 

def display_warehouse_summary(warehouse_summary_df):
    # To make sure we are dealing with a pandas DF
    warehouse_summary_df = pd.DataFrame(warehouse_summary_df)

    with st.sidebar:
        if 'language_selection' not in st.session_state:
            st.session_state['language_selection]'] = ''
        st.markdown('---')
        st.session_state['language_selection]'] = st.selectbox('Detail Display Style', constants.CODE_DISPLAY_OPTIONS)
        # if language_option == 'None':
        #     language_option = ''

    for idx, row in warehouse_summary_df.iterrows():
        st.markdown('---')
        # wh_name = row['name']
        # wh_size = row['size']
        # wh_credits = row['']
        st.markdown('##### ' + row['name'] + '')
        if row['comment']:
            st.caption('Comment: ' + row['comment'])
        wh_col1, wh_col2 = st.columns([1, 1])

        # wh_summary = 'Details\n'
        wh_summary = ''
        wh_summary += 'Owner: ' + row['owner'] + '\n'
        wh_summary += 'Type: ' + row['type'] + '\n'
        wh_summary += 'Size: ' + row['size'] + '\n'
        wh_summary += 'Cluster Min: ' + str(row['min_cluster_count']) + ', Max: ' + str(row['max_cluster_count']) + '\n'
        wh_summary += 'Suspend: ' + str(row['auto_suspend']) + '\n'
        wh_summary += 'Auto Resume: ' + row['auto_resume'] + '\n'
        wh_summary += 'Created: ' + re.sub('\..*', '', str(row['created_on'])) + '\n'
        wh_summary += 'Last Resumed: ' + re.sub('\..*', '', str(row['resumed_on'])) + '\n'
        if row['resource_monitor'] and str(row['resource_monitor']) != 'null':
            wh_summary += 'Resource Monitor: ' + row['resource_monitor'] + '\n'
        wh_summary += 'Scaling Policy: ' + row['scaling_policy'] + '\n'
        wh_summary += 'Query Accel Enabled: ' + row['enable_query_acceleration'] + '\n'
        wh_summary += 'Query Accel Scale Factor: ' + str(row['query_acceleration_max_scale_factor']) + '\n'

        with wh_col1:
            st.markdown('Details')
            # st.markdown('Size: _**' + row['size'] + '**_')
            # st.markdown('Type: _**' + row['type'] + '**_')
            # st.markdown('Cluster Min: `' + str(row['min_cluster_count']) + '` Max: `' + str(row['max_cluster_count']) + '`')
            st.code(wh_summary, language=st.session_state['language_selection]'])

        with wh_col2:
            st.markdown('Recent History Summary')
            if 'TOTAL_QUERIES' in row:
                with st.expander('Queries'):

                    # st.markdown('Total Queries: ' + str(row['TOTAL_QUERIES']))
                    query_history = ''
                    query_history += 'Total Queries: ' + str(row['TOTAL_QUERIES']) + '\n'
                    query_history += 'Distinct Queries: ' + str(row['TOTAL_DISTINCT_QUERIES']) + '\n'
                    query_history += 'Errors: ' + str(row['ERROR_COUNT']) + '\n'
                    st.code(query_history, language=st.session_state['language_selection]'])

                with st.expander('Query Runtime'):
                    total_time = utility.format_seconds_interval(row['TOTAL_QUERY_TIME']/1000)
                    execution_time = utility.format_seconds_interval(row['TOTAL_EXECUTION_TIME']/1000)
                    compile_time = utility.format_seconds_interval(row['TOTAL_COMPILATION_TIME']/1000)
                    provisioning_time = utility.format_seconds_interval(row['TOTAL_QUEUED_PROVISIONING_TIME']/1000)
                    repair_time = utility.format_seconds_interval(row['TOTAL_QUEUED_REPAIR_TIME']/1000)
                    queued_overload_time = utility.format_seconds_interval(row['TOTAL_QUEUED_OVERLOAD_TIME']/1000)
                    transaction_blocked_time = utility.format_seconds_interval(row['TOTAL_TRANSACTION_BLOCKED_TIME']/1000)

                    avg_total_time = utility.format_seconds_interval(row['AVERAGE_QUERY_TIME']/1000)
                    avg_execution_time = utility.format_seconds_interval(row['AVERAGE_EXECUTION_TIME']/1000)
                    avg_compile_time = utility.format_seconds_interval(row['AVERAGE_COMPILATION_TIME']/1000)
                    avg_provisioning_time = utility.format_seconds_interval(row['AVERAGE_QUEUED_PROVISIONING_TIME']/1000)
                    avg_repair_time = utility.format_seconds_interval(row['AVERAGE_QUEUED_REPAIR_TIME']/1000)
                    avg_queued_overload_time = utility.format_seconds_interval(row['AVERAGE_QUEUED_OVERLOAD_TIME']/1000)
                    avg_transaction_blocked_time = utility.format_seconds_interval(row['AVERAGE_TRANSACTION_BLOCKED_TIME']/1000)

                    st.write('Totals')
                    query_history = ''
                    query_history += 'Total: ' + avg_total_time['description'] + '\n'
                    if execution_time['seconds'] > 0:
                        query_history += 'Execution: ' + execution_time['description'] + '\n'
                    if compile_time['seconds'] > 0:
                        query_history += 'Compile: ' + compile_time['description'] + '\n'
                    if provisioning_time['seconds'] > 0:
                        query_history += 'Provisioning: ' + provisioning_time['description'] + '\n'
                    if repair_time['seconds'] > 0:
                        query_history += 'Repair: ' + repair_time['description'] + '\n'
                    if queued_overload_time['seconds'] > 0:
                        query_history += 'Queued Overload: ' + queued_overload_time['description'] + '\n'
                    if transaction_blocked_time['seconds'] > 0:
                        query_history += 'Transaction Blocked: ' + transaction_blocked_time['description'] + '\n'
                    st.code(query_history, language=st.session_state['language_selection]'])

                    st.write('Averages')
                    query_history = ''
                    query_history += 'Avg Total: ' + avg_total_time['description'] + '\n'
                    if avg_execution_time['seconds'] > 0: 
                        query_history += 'Avg Execution: ' + avg_execution_time['description'] + '\n'
                    if avg_compile_time['seconds'] > 0: 
                        query_history += 'Avg Compile: ' + avg_compile_time['description'] + '\n'
                    if avg_provisioning_time['seconds'] > 0: 
                        query_history += 'Avg Provisioning: ' + avg_provisioning_time['description'] + '\n'
                    if avg_repair_time['seconds'] > 0: 
                        query_history += 'Avg Repair: ' + avg_repair_time['description'] + '\n'
                    if avg_queued_overload_time['seconds'] > 0: 
                        query_history += 'Avg Queued Overload: ' + avg_queued_overload_time['description'] + '\n'
                    if avg_transaction_blocked_time['seconds'] > 0: 
                        query_history += 'Avg Transaction Blocked: ' + avg_transaction_blocked_time['description'] + '\n'
                    st.code(query_history, language=st.session_state['language_selection]'])

            if 'CREDITS_USED' in row:
                with st.expander('Usage'):
                    st.markdown('Total Credits: ' + str(row['CREDITS_USED']))

    return True

def main():
    st.markdown('### Warehouse Tagging')

    warehouse_df = pd.DataFrame()

    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if not st.session_state['authenticated']:
        sesh.display_authentication()
        if st.session_state['authenticated']:
            st.experimental_rerun()

    if st.session_state['authenticated']:
        context_container = st.container()
        with st.sidebar:
            st.markdown('#### Context Selection')
            context = sesh.manage_context('narrow')
        with context_container:
            st.caption(sesh.format_context(context))

        wh_option_col1, wh_option_col2 = st.columns([1, 3])

        with wh_option_col1:
            summary_days = st.slider('Days of Recent History', 0, 30, 7, step=1, help="0 indicates today's history only")

        with wh_option_col2:
            wh_search_val = st.text_input('Warehouse Search', value='', placeholder='Warehouse Search', label_visibility='collapsed')
        # if 'role'
        warehouse_df = pd.DataFrame(sesh.cache_sql_disk('show warehouses', st.session_state['account'], context['role']))
        wh_summary_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_QUERY_HISTORY_SUMMARY.format(date_range = str(summary_days)), st.session_state['account'], context['role']))
        wh_tags_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_TAG_SUMMARY, st.session_state['account'], context['role']))
        wh_metering_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_METERING_SUMMARY.format(date_range = str(summary_days)), st.session_state['account'], context['role']))

        warehouse_df = warehouse_df.merge(wh_summary_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')
        warehouse_df = warehouse_df.merge(wh_tags_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')
        warehouse_df = warehouse_df.merge(wh_metering_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')

        warehouse_df['TAG_JSON'] = warehouse_df['TAG_JSON'].fillna('[]')

        if wh_search_val:
            warehouse_df = warehouse_df[warehouse_df.apply(lambda row: row.astype(str).str.contains(wh_search_val, case=False).any(), axis=1)]

        with wh_option_col2:
            wh_filter_list = st.multiselect('Select Warehouse', warehouse_df['name'], default=None, label_visibility='collapsed')

        if wh_filter_list:
            warehouse_df = warehouse_df[warehouse_df['name'].isin(wh_filter_list)]


        display_warehouse_summary(warehouse_df)



    st.table(warehouse_df)

    # st.json(st.session_state)
    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Warehouse Tagging',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    main()