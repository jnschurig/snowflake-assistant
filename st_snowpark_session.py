import streamlit as st
import pandas as pd
import snowflake.snowpark as sp 
from snowflake.snowpark.exceptions import SnowparkSQLException
import constants
import time

change_log = ''' 
1.0.0 - 2023-01-31
- Snowflake session management and authentication
- Query execution
- Context management
- Query result caching
'''

def format_account_url(unformatted_sf_url):
    # Fix the standard url...
    new_url = unformatted_sf_url.replace('https://', '').replace('.snowflakecomputing.com', '')

    # https://ABC12345.prod3.us-west-2.aws.snowflakecomputing.com
    # https://app.snowflake.com/prod3.us-west-2.aws/ABC12345/worksheets

    # Now check for a copied SnowSight URL...
    # if 'app.snowflake.com' in new_url:
    #     # This is an annoying format we need to really work with.
    #     new_url = new_url.replace('app.snowflake.com/', '')
    #     new_url = new_url.split('/')
    #     # 0 = region
    #     # 1 = locator
    #     if '.' in new_url[0]:
    #         # We have some partial stuff here now.
    #         part2 = ''

    return new_url

@st.cache_resource(show_spinner=False)
def create_session(creds_dict):
    ''' 
    Expecting a dict object that contains user credentials. 
    Example:
    {
        'account': 'a Snowflake url string',
        'user': 'user_name',
        'password': 'user_password',
        'role': 'role_name',
        'warehouse': 'wh_name',
        'context': 'database.schema'
    }
    'role' and 'warehouse' are optional. Defaults will be 
    provided in the constants.py file.
    '''
    if 'account' in creds_dict and 'user' in creds_dict and ('pass' in creds_dict or 'password' in creds_dict):
        # Create a session and return it.

        # Format the url into an account idenifier.
        creds_dict['account'] = format_account_url(creds_dict['account'])

        if 'pass' in creds_dict and 'password' not in creds_dict:
            creds_dict['password'] = creds_dict.pop('pass')

        # Connect to Snowflake and set some streamlit session parameters
        with st.spinner('Connecting...'):
            new_snowflake_session = sp.Session.builder.configs(creds_dict).create()
            st.success('Connected!')
        return new_snowflake_session
    return False

def display_authentication(label_visibility_setting='visible'):
    ''' 
    Run this function to display an authentication page. 
    It will display account, user, password, role, and 
    warehouse fields. 
    ''' 
    # Initialize variables and sessions state values
    creds = {}
    if 'authenticated' not in st.session_state: 
        st.session_state['authenticated'] = False

    with st.form('snowflake_credentials'):
        main_url = st.text_input('Account', '', placeholder='Account', label_visibility=label_visibility_setting, help='The qualified account locator or URL for the account. https://docs.snowflake.com/en/user-guide/admin-account-identifier.html')
        
        col1, col2 = st.columns(2)
        with col1:
            main_user = st.text_input('User', '', placeholder='User', label_visibility=label_visibility_setting, help='Snowflake user ID.')
            main_pass = st.text_input('Password', '', placeholder='Password', label_visibility=label_visibility_setting, type='password', help='Password for this user. This will not be stored at any time, ever.')

        with col2:
            main_role = st.text_input('Role', '', placeholder='Role', label_visibility=label_visibility_setting, help='The role to use when logging in. If left blank, the user default will be used.')
            main_wh = st.text_input('Warehouse', '', placeholder='Warehouse', label_visibility=label_visibility_setting, help='The name of the warehouse to use for this session. If left blank, the user default will be used.')
            main_context = st.text_input('Context', '', placeholder='Context', label_visibility=label_visibility_setting, help='The context to use for the connection. Can be a database name or database and schema.')

        # The button goes last, and will always be at the bottom of the first column
        with col1:
            if st.form_submit_button('Connect'):
                st.cache_resource.clear()
                creds['main'] = {
                    'account': main_url,
                    'user': main_user,
                    'password': main_pass
                    }

                if main_role:
                    creds['main']['role'] = main_role
                if main_role:
                    creds['main']['warehouse'] = main_wh
                if main_context:
                    if '.' in main_context:
                        main_context = main_context.split('.')
                        if len(main_context) > 1:
                            creds['main']['database'] = main_context[0]
                            creds['main']['schema'] = main_context[1]
                            
                st.session_state['main_session'] = create_session(creds['main'])
                st.session_state['authenticated'] = True
                st.session_state['account'] = creds['main']['account']
                st.session_state['user'] = creds['main']['user']

                if 'role' in creds['main']:
                    st.session_state['role'] = creds['main']['role']
                else:
                    # Get the role from the connection?
                    # It will be whatever default role the user has set up.
                    # st.session_state['role']
                    pass

        if st.session_state['authenticated']:
            return st.session_state['main_session']
    return True

def run_sql(sql):
    ''' 
    Main function for running a query against Snowflake. 
    Returns a Snowflake dataframe (slightly different 
    from a Pandas dataframe). Includes error handling to 
    re-authenticate if the token has expired. 
    '''
    # Do a bit of error handling, just in case...
    if 'main_session' in st.session_state:
        try:
            return st.session_state['main_session'].sql(sql).collect()
        except SnowparkSQLException as err:
            if err.error_code == '1304': # Token is expired
                st.session_state['authenticated'] = False
                del st.session_state['main_session']
                st.experimental_rerun()
            else:
                raise err
    else:
        st.error('No valid session')
        return None

@st.cache_data(persist='disk', show_spinner=False)
def cache_sql_disk(sql, account='account', role=''):
    ''' 
    Cache the results of a sql result to disk, which 
    is stored more or less permanently. Typically 
    used for large data sets which are not likely to 
    change often.
    ''' 
    # Adding account as an input so results are cached separately for each account.
    with st.spinner('Running query...'):
        if 'main_session' in st.session_state:
            try:
                return st.session_state['main_session'].sql(sql).collect()
            except SnowparkSQLException as err:
                if err.error_code == '1304': # Token is expired
                    st.session_state['authenticated'] = False
                    del st.session_state['main_session']
                    st.experimental_rerun()
        # else:
        #     st.error('No valid session')
        #     return None

@st.cache_data(persist=None, ttl=constants.MEMORY_CACHE_MAX_AGE_SECONDS, show_spinner=False)
def cache_sql_memory(sql, account='account', role=''):
    ''' 
    Cache the results of a sql result in memory for up 
    to 600 seconds (10 minutes). Typically used for small 
    to medium qeuries that you might want to refresh 
    during a single session in the app.
    ''' 
    # Adding account as an input so results are cached separately for each account.
    if 'main_session' in st.session_state:
        try:
            return st.session_state['main_session'].sql(sql).collect()
        except SnowparkSQLException as err:
            if err.error_code == '1304': # Token is expired
                st.session_state['authenticated'] = False
                del st.session_state['main_session']
                st.experimental_rerun()
    # else:
    #     st.error('No valid session')
    #     return None

def clear_all_cache():
    ''' 
    This doesn't need to be in a function here. 
    It is included mainly as a convenience so 
    the target script might not HAVE to import 
    streamlit.
    ''' 
    # Clears all cached content on this server. 
    # st.experimental_memo.clear()
    st.cache_data.clear()
    return True

def close_session():
    ''' 
    End all running queries and close the connection to Snowflake.
    ''' 
    if 'main_session' in st.session_state:
        st.session_state['main_session'].close()

    del st.session_state['main_session']
    st.session_state['authenticated'] = False

    return True

def get_first_result(sql_text, return_column=0):
    ''' 
    Run a SQL statement and get a value from a 
    single on the first row. Defaults to first 
    column. Can specify column by index number 
    (starting with 0), or by name as a string.
    '''
    result = run_sql(sql_text)

    return result[0][return_column]

def sort_dropdown_with_default(dataframe, default_value, column=0):
    ''' 
    Returns a list with a specified value first in the list.
    Inputs
    dataframe    : a Pandas DataFrame object
    default_value: Value from a column you would to appear first
    column       : The column to use for values. Can specify name or number
    '''
    # Returns a list
    iterable = [default_value]
    df = pd.DataFrame(dataframe)
    for index, row in df.iterrows():
        if row[column] not in iterable:
            iterable.append(row[column])

    return iterable

def context_selector(context_type):
    ''' 
    Dynamically display a context selector. Valid context types 
    come from constants.VALID_CONTEXT_TYPES
    '''
    new_context = st.selectbox(
        context_type, 
        sort_dropdown_with_default(run_sql('show ' + context_type + 's'), st.session_state['current_context'][context_type.lower()], column='name'), 
        index=0, 
        key=context_type.lower() + '_selection'
    )
    if new_context != st.session_state['current_context'][context_type.lower()]:
        run_sql('use ' + context_type + ' ' + str(new_context))
        st.experimental_rerun()
    return True

def manage_context(display_format='standard'):
    ''' 
    Display a container with Snowflake context display 
    and selection options. Context options include role, 
    warehouse, database, and schema. The display format 
    can be modified depending on where the context is 
    displayed. Valid display formats can be found in 
    constants.FORMAT_COLUMN_COUNT. For displaying 
    in the sidebar, use 'narrow'. For display in the main 
    page section, use 'standard'. If the page is set to 
    wide mode, try 'wide'.
    '''
    with st.container():
        if display_format not in list(constants.FORMAT_COLUMN_COUNT.keys()):
            st.error('Error: display format not valid. Please choose a valid format in: ' + str(constants.FORMAT_COLUMN_COUNT.keys()))

        column_count = constants.FORMAT_COLUMN_COUNT[display_format]
        if 'current_context' not in st.session_state:
            st.session_state['current_context'] = {}

        st.session_state['current_context']['role'] = str(st.session_state['main_session'].get_current_role()).replace('"', '')
        st.session_state['current_context']['warehouse'] = str(st.session_state['main_session'].get_current_warehouse()).replace('"', '')
        st.session_state['current_context']['database'] = str(st.session_state['main_session'].get_current_database()).replace('"', '')
        st.session_state['current_context']['schema'] = str(st.session_state['main_session'].get_current_schema()).replace('"', '')

        context_cols = st.columns(column_count)
        for index, context in enumerate(constants.VALID_CONTEXT_TYPES):
            col = context_cols[index % column_count]
            with col:
                context_selector(context)

        return st.session_state['current_context']
    
def format_context(context_dict):
    display_context = ''
    if len(context_dict) > 0:
        display_context = '👤 ' + context_dict['role'] 
        if context_dict['warehouse']:
            display_context += ' | 🖧 ' + context_dict['warehouse']
        if context_dict['database'] and str(context_dict['database']) != 'None':
            display_context += ' | ' + str(context_dict['database'])
        if context_dict['schema'] and str(context_dict['schema']) != 'None':
            display_context += ' > ' + str(context_dict['schema'])

    return display_context
    
def main_handler(label_visibility_setting='visible'):
    context = {}

    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    with st.sidebar:
        st.markdown('**Session Context**')
        if st.session_state['authenticated']:
            context = manage_context('narrow')

        if st.button('Disconnect Snowflake'):
            st.session_state['main_session'].close()
            st.session_state['authenticated'] = False
            st.experimental_rerun()

    if not st.session_state['authenticated']:
        st.markdown('### Snowflake Authentication')
        display_authentication(label_visibility_setting)
        if st.session_state['authenticated']:
            st.experimental_rerun()
    
    return context

def initialize_assistant_db(schema='tagging', database=constants.DEFAULT_DATABASE):
    # See about initializing the application database and schema
    initialize_db = False

    db_exists = run_sql("show databases like '" + database + "'")

    if len(db_exists) == 0:
        initialize_db = True
    else:
        schema_exists = run_sql("show schemas like '" + schema + "' in database " + database)
        if len(schema_exists) == 0:
            initialize_db = True 

    if initialize_db:
        st.warning('Warning: Recommended DB/Schema not found: ' + database + '.' + schema)
        with st.expander('DB/Schema Initialization', expanded=True):
            with open('snowflake_assistant_initialization.sql', 'r') as f:
                init_sql = f.read()

            init_sql = init_sql.format(
                security_object_role = constants.SECURITY_OBJECT_ROLE, 
                database_object_role = constants.DATABASE_OBJECT_ROLE, 
                snowflake_assistant_db = database, 
                schema_name = schema
            )
            contains_error = False
            if st.button('Execute', key='execute_init_script', help='Execute the below script as shown. Do at your own risk.'):
                with st.spinner('Executing...'):
                    script_list = init_sql.split(';')
                    for script in script_list:
                        if script.strip() != '':
                            try:
                                st.session_state['main_session'].sql(script).collect()
                            except SnowparkSQLException as err:
                                st.error(err)
                                contains_error = True
                if not contains_error:
                    st.success('Initialized!')
                    time.sleep(3)
                    st.experimental_rerun()

            st.code(init_sql, language='sql')

            if contains_error and st.button('Reload Page'):
                st.experimental_rerun()

    # else:
    #     st.success('All components found. Proceeding')

    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Session Management',
        layout='centered',
        initial_sidebar_state='collapsed',
    )
    st.write('Test Section for Session and Basic SQL Execution')
    display_authentication()

    with st.sidebar:
        if st.radio('Context Controls', (False, True)):
            st.markdown('**Session Context**')
            if st.session_state['authenticated']:
                manage_context('narrow')

            if st.button('Disconnect Snowflake'):
                st.session_state['main_session'].close()
                st.session_state['authenticated'] = False
                st.experimental_rerun()

    if st.session_state['authenticated']:
        sql_text = st.text_area('SQL', placeholder='select * from ...')
        st.code(sql_text, language='sql')
        
        run_sql, run_memory, run_disk, clear_cache = st.columns(4)

        sql_result = None
        with run_sql:
            if st.button('Just run the sql'):
                sql_result = st.session_state['main_session'].sql(sql_text).collect()

        with run_memory:
            if st.button('Run with memory caching'):
                sql_result = cache_sql_memory(sql_text, st.session_state['account'], st.session_state['current_context']['role'])

        with run_disk:
            if st.button('Run with disk caching'):
                sql_result = cache_sql_disk(sql_text, st.session_state['account'], st.session_state['current_context']['role'])

        with clear_cache:
            if st.button('Clear cache'):
                clear_all_cache()

        if sql_result is not None:
            st.write('SQL Results')
            st.table(sql_result)

    st.markdown('---')
    st.markdown('Session State')
    st.json(st.session_state, expanded=False)
