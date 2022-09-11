import streamlit as st
import snowflake.snowpark as sp
# from snowflake.snowpark.functions import col, lit
import pandas as pd
# import seaborn as sns 
import matplotlib.pyplot as plt
import constants, utility, re
import json

change_log = '''
1.1.0 - 2022-09-10
- Added some usage stats to Warehouse display
---
1.0.0 - 2022-08-22
- First version debut!
- Warehouse creation and editing
- Tag creation
- Apply tags and get "recommended" values
'''

def create_session(creds):
    ''' 
    Expecting a dict object that contains user credentials. 
    Example:
    {
        'url': 'a url string',
        'user': 'user_name',
        'pass': 'user_password',
        'role': 'role_name',
        'warehouse': 'wh_name'
    }
    'role' and 'warehouse' are optional. Defaults will be 
    provided in the constants.py file.
    '''
    if 'url' in creds and 'user' in creds and ('pass' in creds or 'password' in creds):
        # Create a session and return it.
        connection_params = {
            'account': creds['url'].replace('https://', '').replace('.snowflakecomputing.com', ''),
            'user': creds['user']
        }

        if 'pass' in creds:
            connection_params['password'] = creds['pass']
        elif 'password' in creds:
            connection_params['password'] = creds['password']

        if 'role' in creds:
            connection_params['role'] = creds['role']
        else: connection_params['role'] = constants.DEFAULT_ROLE

        if 'warehouse' in creds:
            connection_params['warehouse'] = creds['warehouse']
        else:
            connection_params['warehouse'] = constants.DEFAULT_WAREHOUSE

        with st.spinner('Connecting...'):
            use_session = sp.Session.builder.configs(connection_params).create()
            st.success('Connected!')
        return use_session
    return False

@st.experimental_memo(persist='disk', ttl=constants.DISK_CACHE_MAX_AGE_SECONDS)
def cache_large_sql(sql, account):
    # Adding account as an input so that it is cached separately for each account.
    if 'main_session' in st.session_state:
        return st.session_state['main_session'].sql(sql).collect()
    else:
        return None

@st.experimental_memo(persist=None, ttl=constants.MEMORY_CACHE_MAX_AGE_SECONDS, show_spinner=False)
def cache_small_sql(sql, account):
    # Adding account as an input so that it is cached separately for each account.
    if 'main_session' in st.session_state:
        return st.session_state['main_session'].sql(sql).collect()
    else:
        return None

def go():
    ''' 
    A main container function which contains the body of the app. 
    Containerized here so that it can be included in another app 
    if desired.
    '''
    default_state = {
        'debug': False,
        'authenticated': False
    }

    for key in default_state.keys():
        if key not in st.session_state:
            st.session_state[key] = default_state[key]

    st.set_page_config(
        page_title='Tagging Assistant',
        layout='centered',
        initial_sidebar_state='collapsed',
        menu_items={
            'About': constants.APP_ABOUT_TEXT
        }
    )

    st.header('Tagging Assistant')

    with st.sidebar:
        with st.expander('New to the Assistant?', False):
            st.markdown(constants.APP_INFO_TEXT)

        with st.expander('Additional Options', False):
            # if st.button('Disconnect', help='Disconnect current Snowflake session'):
            #     if st.session_state['authenticated']:
            #         st.session_state['main_session'].close()
            #         st.experimental_rerun()

            if st.button('Reset Session', help='Reset all session variables to default values.'):
                if st.session_state['authenticated']:
                    st.session_state['main_session'].close()
                    del st.session_state['main_session']
                    st.session_state['authenticated'] = False
                    
                for key in st.session_state.keys():
                    if key in default_state.keys():
                        st.session_state[key] = default_state[key]
                    else:
                        del st.session_state[key]
            if st.button('Toggle DEBUG', help='Displays additional detail at the bottom of the page.'):
                if st.session_state.debug:
                    st.session_state.debug = False 
                else:
                    st.session_state.debug = True

        with st.expander('Change Log', False):
            st.caption(change_log)

    tab1, tab2, tab3, tab4 = st.tabs(['Authentication', 'Warehouses', 'Tags', 'Apply Tags'])

    with tab1:
        st.subheader('Authentication', 'auth')

        with st.expander('Pre-installation Scripts'):
            st.markdown(constants.PRE_INSTALL_PROMISE_TEXT)
            with open('snowflake_pre_script.sql', 'r') as f:
                pre_install_sql = f.read()

            st.code(pre_install_sql, language='sql')

        creds = {}
        creds['main'] = {}
        with st.form('main_creds'):
            main_url = st.text_input('Account', '', help='The qualified account locator or URL for the account.')
            
            col1, col2 = st.columns(2)
            with col1:
                main_user = st.text_input('User', '', help='User with `sysadmin` or `accountadmin` role assigned.')
            with col2:
                main_pass = st.text_input('Password', '', type='password', help='Password for this user. This will not be stored at any time, ever.')

            if st.form_submit_button('Connect'):
                creds['main'] = {
                    'url': main_url,
                    'user': main_user,
                    'password': main_pass
                    }

                st.session_state['main_session'] = create_session(creds['main'])
                st.session_state['authenticated'] = True
                    

    if st.session_state['authenticated']:
        with tab2:
            st.subheader('Warehouses', 'wh')

            with st.spinner('Getting Warehouses'):
                # account_warehouses_df = st.session_state['main_session'].sql('show warehouses').collect()
                account_warehouses_df = cache_large_sql('show warehouses', main_url)

            wh_col1, wh_col2 = st.columns(2, gap='medium')

            with wh_col1: 
                assistant_enabled_setting = st.radio('Assistant Enabled', ('All', 'Yes', 'No'), key='assistant_enabled_setting', help='Filter warehouse list based on whether Assistant is enabled on that warehouse already.')

                if st.button('Refresh Warehouse List', key='refresh_wh_button', help='Clears the cache, so all large data sources will be rerun. You may need to push the button a second time to trigger a reload.'):
                    st.experimental_memo.clear()
                    st.experimental_rerun()

                warehouse_list = ['']
                wh_lookup = {}
                with st.spinner('Getting Warehouse Information...'):
                    for row in account_warehouses_df:
                        wh_name = row['name']
                        wh_lookup[wh_name] = {}
                        wh_lookup[wh_name]['name'] = wh_name

                        assist_is_enabled_wh = cache_small_sql("select nvl(system$get_tag('tagging_assist_db.tagging.tag_assistant_enabled', '" + wh_name + "', 'warehouse'), 'n') as enabled", main_url)
                        assist_is_enabled_wh = assist_is_enabled_wh[0]['ENABLED']
                        wh_lookup[wh_name]['assist_enabled'] = assist_is_enabled_wh

                        if assistant_enabled_setting == 'All':
                            warehouse_list.append(wh_name)
                        elif assistant_enabled_setting == 'Yes' and assist_is_enabled_wh == 'y':
                            warehouse_list.append(wh_name)
                        elif assistant_enabled_setting == 'No' and assist_is_enabled_wh == 'n':
                            warehouse_list.append(wh_name)

                        wh_lookup[wh_name]['state'] = row['state']
                        wh_lookup[wh_name]['type'] = row['type']
                        wh_lookup[wh_name]['size'] = row['size']
                        wh_lookup[wh_name]['auto_suspend'] = row['auto_suspend']
                        wh_lookup[wh_name]['auto_resume'] = row['auto_resume']
                        wh_lookup[wh_name]['owner'] = row['owner']
                        wh_lookup[wh_name]['comment'] = row['comment'][:constants.COMMENT_MAX_LENGTH]
                        wh_lookup[wh_name]['clustering'] = {}
                        wh_lookup[wh_name]['clustering']['scaling_policy'] = row['scaling_policy']
                        wh_lookup[wh_name]['clustering']['min'] = row['min_cluster_count']
                        wh_lookup[wh_name]['clustering']['max'] = row['max_cluster_count']
                        wh_lookup[wh_name]['clustering']['started'] = row['started_clusters']
                        wh_lookup[wh_name]['clustering']['running'] = row['running']
                        wh_lookup[wh_name]['query_acceleration'] = {}
                        wh_lookup[wh_name]['query_acceleration']['enabled'] = row['enable_query_acceleration']
                        wh_lookup[wh_name]['query_acceleration']['max_scale_factor'] = row['query_acceleration_max_scale_factor']
                    
            selected_wh = st.selectbox('Select', warehouse_list, key='selected_wh', help='Warehouses found in the account which are available to `sysadmin`')
                
            with wh_col2:
                if selected_wh != '':
                    st.markdown('Name: **' + selected_wh + '**')
                    st.markdown('Current Size: **' + wh_lookup[selected_wh]['size'] + '**')
                    auto_suspend_breakdown = utility.format_seconds_interval(wh_lookup[selected_wh]['auto_suspend'])
                    st.markdown('Auto Suspend: **' + str(auto_suspend_breakdown['total_seconds']) + ' seconds** (' + auto_suspend_breakdown['description'] + ')')

                    with st.expander('Additional Detail'):
                        st.json(wh_lookup[selected_wh])

                    if wh_lookup[selected_wh]['assist_enabled'] == 'n':
                        if st.button('Enable Assistant', help='Enable management of this warehouse by the Assistant. Clicking this button will _**not**_ remove existing permissions or change any warehouse settings. Clicking this button _**will**_ add a tag and value to this warehouse.'):
                            enable_sql = "alter warehouse " + selected_wh + " set tag tagging_assist_db.tagging.tag_assistant_enabled = 'y'" 
                            st.session_state['main_session'].sql(enable_sql).collect()
                            wh_lookup[selected_wh]['assist_enabled'] = 'y'
                            st.success('Enabled')
                    else:
                        # Let's manage this thing a bit...
                        if st.button('Disable Assistant', help='Disable management of this warehouse by the Assistant. Clicking this button will _**not**_ remove existing permissions or change any warehouse settings. Clicking this button _**will**_ modify a tag and value on this warehouse.'):
                            disable_sql = "alter warehouse " + selected_wh + " set tag tagging_assist_db.tagging.tag_assistant_enabled = 'n'" 
                            st.session_state['main_session'].sql(disable_sql).collect()
                            wh_lookup[selected_wh]['assist_enabled'] = 'n'
                            st.warning('Disabled')

            # st.markdown('---')
            with st.container():
                st.write('Average Credit Usage Over 30 Days')
                wh_stats1, wh_stats2 = st.columns(2)
                with st.spinner('Getting Usage Stats...'):
                    # warehouse_usage_stats = pd.DataFrame(cache_large_sql('''select start_day_name, round(sum(credits_used), 2)::float as credits_used from tagging_assist_db.metadata.warehouse_usage_last_month group by start_day_name ''', main_url))
                    warehouse_usage_stats = pd.DataFrame(cache_large_sql('''select warehouse_name, start_day_name, start_hour, round(credits_used, 2)::float as credits_used from tagging_assist_db.metadata.warehouse_usage_last_month order by 1, 2, 3 ''', main_url))
                    if selected_wh != '':
                        warehouse_usage_stats = warehouse_usage_stats[warehouse_usage_stats['WAREHOUSE_NAME'] == selected_wh]
                with wh_stats1:
                    st.area_chart(warehouse_usage_stats.groupby(['START_DAY_NAME'], as_index=False).mean(), x='START_DAY_NAME', y='CREDITS_USED')

                with wh_stats2:
                    st.area_chart(warehouse_usage_stats.groupby(['START_HOUR'], as_index=False).mean(), x='START_HOUR', y='CREDITS_USED')

            if selected_wh != '' and wh_lookup[selected_wh]['assist_enabled'] == 'y':
                with st.form('warehouse_settings', clear_on_submit=True):
                    st.subheader('Edit ' + selected_wh + ' Settings')
                    whcol1, whcol2 = st.columns([5, 2], gap='medium')
                    # whcol1, whcol2 = st.columns([3, 1], gap='medium')
                    with whcol2:
                        # Display a table showing wh size credit cost
                        st.code(utility.format_wh_usage(with_header=True))
                        st.caption('Actual cost of credits (in currency) will vary based on cloud provider and region. 5x and 6x warehouses may not be available in your region.')

                    with whcol1:
                        new_wh_size = st.select_slider('Warehouse Size', constants.WAREHOUSE_SIZES, value=wh_lookup[selected_wh]['size'])

                        suspend_lookup = {}
                        for val in constants.WAREHOUSE_AUTO_SUSPEND_STEPS:
                            val_lookup = utility.format_seconds_interval(val)
                            if val == 0:
                                suspend_lookup[val_lookup['status']] = val
                            else:
                                suspend_lookup[val_lookup['description']] = val

                        new_wh_suspend_seconds = st.select_slider('Auto Suspend (current setting is ' + auto_suspend_breakdown['description'] + ')', suspend_lookup, value='1 minute')
                        new_wh_suspend_seconds = suspend_lookup[new_wh_suspend_seconds]
                        # st.write(new_wh_suspend_seconds)
                        # st.json(suspend_lookup)


                        new_wh_auto_resume = st.radio('Auto Resume', (True, False), help='If set to True, the warehouse will automatically resume when queries are executed against it.')

                        new_wh_comment = st.text_area('Comment', max_chars=constants.COMMENT_MAX_LENGTH, value=wh_lookup[selected_wh]['comment'], help='Add a comment to the Warehouse Definition')

                    with st.expander('Clustering', False):
                        new_wh_scaling_pol = st.radio('Scaling Policy', ('Standard', 'Economy'), help='Standard will attempt to minimize queueing by starting clusters. Conserves credits by allowing some queueing.')
                        cluster_col1, cluster_col2 = st.columns(2)
                        with cluster_col1:
                            new_wh_cluster_min = st.slider('Minimum Clusters', min_value=1, max_value=10, value=wh_lookup[selected_wh]['clustering']['min'], help='Clustering only supported on Enterprise or higher editions of Snowflake. Leave at 1 if needed.')
                        with cluster_col2:
                            new_wh_cluster_max = st.slider('Maximum Clusters', min_value=1, max_value=10, value=wh_lookup[selected_wh]['clustering']['max'], help='Clustering only supported on Enterprise or higher editions of Snowflake. Leave at 1 if needed.')

                    # with st.expander('Query Acceleration', False):
                    query_acc_col1, query_acc_col2 = st.columns([2, 7])
                    with query_acc_col1:
                        new_wh_query_acc = st.radio('Enable Acceleration', (False, True), help='Allow the warehouse to automatically scale vertically. If in doubt, leave as False')

                    with query_acc_col2:
                        new_wh_query_acc_scaling = st.slider('Scale Factor', min_value=0, max_value=100, value=wh_lookup[selected_wh]['query_acceleration']['max_scale_factor'], help='Unless Query Accleration is enabled, this setting does nothing. Allows resource scaling as a multiple of this number. If set to 2, the base warehouse size can scale up to 2x its normal resource amount.')

                    if st.form_submit_button('Alter Warehouse'):
                        wh_alteration_query  = "alter warehouse " + selected_wh + ' \n'
                        wh_alteration_query += "  set warehouse_size = " + constants.WAREHOUSE_SIZES[new_wh_size]['code'] + ' \n'
                        wh_alteration_query += "     ,auto_suspend = " + str(new_wh_suspend_seconds) + " \n"
                        wh_alteration_query += "     ,auto_resume = " + str(new_wh_auto_resume).lower() + " \n"
                        if new_wh_comment.strip() != '':
                            wh_alteration_query += "     -- The '$$' are Snowflake SQL for a multi-line string \n"
                            wh_alteration_query += "     ,comment = $$" + new_wh_comment + "$$ \n"
                        wh_alteration_query += "     ,scaling_policy = " + new_wh_scaling_pol.upper() + " \n"
                        wh_alteration_query += "     ,min_cluster_count = " + str(new_wh_cluster_min) + " \n"
                        wh_alteration_query += "     ,max_cluster_count = " + str(new_wh_cluster_max) + " \n"
                        wh_alteration_query += "     ,enable_query_acceleration = " + str(new_wh_query_acc).lower() + " \n"
                        wh_alteration_query += "     ,query_acceleration_max_scale_factor = " + str(new_wh_query_acc_scaling) + " \n"
                        if st.session_state['debug']: st.code(wh_alteration_query, language='sql')
                        with st.spinner('Altering...'):

                            alter_result = st.session_state['main_session'].sql(wh_alteration_query).collect()
                            st.success(alter_result[0]['status'])

            with st.form('create_wh_form', clear_on_submit=True):
                st.subheader('Create New Warehouse', anchor='create_wh')
                new_wh_col1, new_wh_col2 = st.columns(2)
                with new_wh_col1:
                    new_wh_name = st.text_input('Name', key='new_wh_name', help='Input a database-friendly name to call the new warehouse.')

                with new_wh_col2:
                    new_wh_owner = st.text_input('Owner', key='new_wh_owner', value='SYSADMIN', help='The Snowflake role to which ownership will be assigned after creation.')
                if st.form_submit_button('Create Warehouse', help='Create a new warehouse with default settings. Use Edit Warehouse interface to customize.'):
                    with st.spinner('Creating...'):
                        create_wh_result = st.session_state['main_session'].sql('create warehouse if not exists ' + new_wh_name).collect()
                        if new_wh_owner.lower() != 'sysadmin':
                            st.session_state['main_session'].sql('grant ownership on warehouse ' + new_wh_name + ' to role ' + new_wh_owner + ' copy current grants').collect()
                        st.success(create_wh_result[0]['status'])
        with tab3:
            st.subheader('Tags', 'tags')

            available_tags_df = st.session_state['main_session'].sql('show tags in tagging_assist_db.tagging').collect()
            # available_tags_df = cache_large_sql('show tags in tagging_assist_db.tagging')
            if st.button('Refresh Tags', key='refresh_tags_button', help='Will re-acquire the list of tags. '):
                # Triggers a re-draw of the page, so it will refresh the data even though this button doesn't actually do anything.
                pass

            
            # st.markdown('---')
            tag_list = ['']
            tag_lookup = {}
            for row in available_tags_df:
                tag_key = row[1]
                tag_list.append(tag_key)
                tag_lookup[tag_key] = {}
                tag_lookup[tag_key]['name'] = row[1]
                # Disabling this for now because I don't want to deal with a datetime object
                # tag_lookup[tag_key]['created_on'] = row[0] 
                tag_lookup[tag_key]['database_name'] = row[2]
                tag_lookup[tag_key]['schema_name'] = row[3]
                tag_lookup[tag_key]['owner'] = row[4]
                tag_lookup[tag_key]['comment'] = row[5]
                # tag_lookup[tag_key]['allowed_values'] = json.loads(row[6])
                tag_lookup[tag_key]['allowed_values'] = []
                if row[6]:
                    tag_lookup[tag_key]['allowed_values'] = json.loads(row[6])

            tag_col1, tag_col2, tag_col3, tag_col4 = st.columns([3, 3, 1, 1])

            for key in tag_lookup.keys():
                if key == 'TAG_ASSISTANT_ENABLED':
                    continue

                with tag_col1:
                    st.write(key)

                with tag_col2:
                    st.write('Allowed Values: ' + utility.convert_list_string(tag_lookup[key]['allowed_values']))

                with tag_col3:
                    want_to_del_tag = st.checkbox('Del', key=key+'_del_check', help='Drop this tag from the database.')

                with tag_col4:
                    if want_to_del_tag:
                        if st.button('Confirm', key=key+'_confirm_del', help='Clicking this button will permanently drop this tag and dissociate it from all objects in the account. Do not push this unless you mean it!'):
                            drop_tag_result = st.session_state['main_session'].sql('drop tag if exists tagging_assist_db.tagging.' + key).collect()
                            # st.success(drop_tag_result[0]['status'])
                            st.success('Dropped')
                    else:
                        st.write('...')

            with st.form('create_tag_form', clear_on_submit=False):
                new_tag_name = st.text_input('Tag Name', max_chars=100, key='new_tag_name', help='Database-friendly name of the new tag. Name should contain only letters, numbers, and underscores.')
                new_tag_allowed_values = st.text_input('Allowed Values (Optional)', key='new_tag_allowed_values', help='Comma separated list of possible allowed values for this tag. Use with caution.')
                new_tag_comment = st.text_area('Comment (Optional)', max_chars=constants.COMMENT_MAX_LENGTH, key='new_tag_comment', help='Add a comment to the new tag.')

                # new_tag_col1, new_tag_col2 = st.columns([1, 5])
                # with new_tag_col2:
                new_tag_replace = st.checkbox('Replace if already exists', value=False, key='new_tag_replace', help='Check this box if you want to create this tag even if it already exists.')

                if st.form_submit_button('Create Tag'):
                    # with new_tag_col1:
                    create_tag_sql = ''
                    if new_tag_replace:
                        create_tag_sql += 'create or replace tag '
                    else:
                        create_tag_sql += 'create tag if not exists '
                    create_tag_sql += 'tagging_assist_db.tagging.' + new_tag_name


                    if new_tag_allowed_values != '':
                        new_tag_allowed_values = utility.convert_list_string(new_tag_allowed_values)
                        new_tag_allowed_values = utility.convert_list_string(new_tag_allowed_values, remove_quotes=False)
                        create_tag_sql += '\n  allowed_values ' + new_tag_allowed_values
                        # st.write(new_tag_allowed_values)

                    if new_tag_comment != '':
                        create_tag_sql += '\n  comment = $$' + new_tag_comment + '$$'

                    if st.session_state['debug']: st.code(create_tag_sql, language='sql')
                    create_tag_result = st.session_state['main_session'].sql(create_tag_sql).collect()
                    st.success(create_tag_result[0]['status'])
            # st.json(tag_lookup)

        with tab4:
            st.subheader('Apply Tags')

            apply_tag_wh = st.selectbox('Warehouse', warehouse_list, key='apply_tag_wh', help='Warehouses are filtered based on the "Assistant Enabled" option from the "Warehouses" tab.')

            apply_tag_name = st.selectbox('Available Tags', tag_list, key='apply_tag_name')

            with st.form('apply_tag_value'):
                apply_tag_col1, apply_tag_col2 = st.columns(2)

                with apply_tag_col1:
                    apply_tag_value = st.text_input('Tag Value', '', max_chars=100, key='apply_tag_value', help='Help!')
    
                with apply_tag_col2:
                    st.write('Current Value')
                    if apply_tag_wh != '' and apply_tag_name != '':
                        current_tag_value_sql = "select nvl(system$get_tag('tagging_assist_db.tagging." + apply_tag_name + "', '" + apply_tag_wh + "', 'warehouse'), '<none set>') as tag_value"
                        current_tag_value_result = st.session_state['main_session'].sql(current_tag_value_sql).collect()
                        st.markdown('**' + current_tag_value_result[0]['TAG_VALUE'] + '**')

                    st.write('Allowed Values')
                    if apply_tag_name != '' and tag_lookup[apply_tag_name]['allowed_values']:
                        st.markdown('**' + utility.convert_list_string(tag_lookup[apply_tag_name]['allowed_values'], remove_quotes=False) + '**')
                    else:
                        st.markdown('**<any>**')
                
                with apply_tag_col1:
                    if st.form_submit_button('Apply Tag Value'):
                        if apply_tag_value == '':
                            apply_tag_sql = "alter warehouse " + apply_tag_wh + " unset tag tagging_assist_db.tagging." + apply_tag_name
                        else:
                            apply_tag_sql = "alter warehouse " + apply_tag_wh + " set tag tagging_assist_db.tagging." + apply_tag_name + " = $$" + apply_tag_value + "$$"
                        with st.spinner('Applying Tag Value...'):
                            apply_tag_result = st.session_state['main_session'].sql(apply_tag_sql).collect()
                            st.success(apply_tag_result[0]['status'])

            st.warning('Tag values may take up to 3 hours to appear in stats')

            suggested_tag_sql = constants.SUGGESTED_VALUE_SQL + " where tag_name = '" + apply_tag_name + "'"
            if apply_tag_name != '':
                if st.session_state['debug']: st.code(suggested_tag_sql)
                with st.spinner('Searching for Suggestions...'):
                    # suggested_tag_result = st.session_state['main_session'].sql(suggested_tag_sql).collect()
                    suggested_tag_result = cache_small_sql(suggested_tag_sql, main_url)
                    st.write('Suggested Values')
                    st.markdown('**' + suggested_tag_result[0]['SUGGESTED_VALUES'] + '**')


    # Bottom. EVERYTHING goes above this...
    if st.session_state.debug:
        st.markdown('---')
        st.subheader('Debug', 'debug')

        debug_1, debug_2, debug_3 = st.columns(3)
        with debug_1:
            st.write('Session State:')
            st.json(st.session_state)

        with debug_2:
            st.write('Selected Warehouse:')
            if selected_wh != '':
                st.json(wh_lookup[selected_wh])

                st.write('Duration Breakdown')
                st.json(auto_suspend_breakdown)

        with debug_3:
            st.write('Available Tags:')
            st.json(tag_lookup, expanded=False)

    return True

if __name__ == '__main__':
    go()