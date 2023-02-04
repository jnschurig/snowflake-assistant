import pandas as pd
import streamlit as st
import st_snowpark_session as sesh 
import constants
import time, json
from snowflake.snowpark.exceptions import SnowparkSQLException

def tag_creation_form(display_format='centered', label_visibility_setting='visible', show_form_title_text=True):
            
    with st.form(key='tag_creation_form', clear_on_submit=True):
        if show_form_title_text:
            st.markdown('##### New Tag')
        if label_visibility_setting in ['collapsed', 'hidden']:
            st.caption('Tag creation will use current context if not supplied.')
        if display_format not in list(constants.FORMAT_COLUMN_COUNT.keys()):
            st.error('Error: display format not valid. Please choose a valid format in: ' + str(constants.FORMAT_COLUMN_COUNT.keys()))

        column_count = constants.FORMAT_COLUMN_COUNT[display_format]

        form_cols = st.columns(column_count)
        new_tag_dict = {}
        for index, field in enumerate(constants.TAG_FORM_FIELDS):
            col = form_cols[index % column_count]
            if field['type'] == 'text_input':
                with col:
                    new_tag_dict[field['name']] = st.text_input(
                        field['name'], 
                        value='', 
                        key='tag_field_' + str(index), 
                        placeholder=field['name'],
                        label_visibility=label_visibility_setting, 
                        help=field['help']
                    )

        st.caption('*Tag will be created with the __replace__ option.')
        st.caption('**Use __Allowed Values__ field with caution.')
                
        submitted = st.form_submit_button('Create Tag')
        if submitted:
            if not new_tag_dict['Tag Name']:
                st.error('Tag Name is required!')
                submitted = False 
                time.sleep(3)
                st.experimental_rerun()
            else:
                # Assemble the creation script...
                full_tag_name = ''
                if new_tag_dict['Tag Database'] and new_tag_dict['Tag Schema']:
                    full_tag_name += str(new_tag_dict['Tag Database']) + '.'
                if new_tag_dict['Tag Schema']:
                    full_tag_name += str(new_tag_dict['Tag Schema']) + '.'
                full_tag_name += new_tag_dict['Tag Name']
                tag_create_script = 'create or replace tag '
                tag_create_script += full_tag_name
                # tag_create_script += ' comment = $$' + constants.OBJECT_CREATION_COMMENT + '$$'
                if new_tag_dict['Allowed Values'] != '':
                    tag_create_script += ' allowed_values ' + str(new_tag_dict['Allowed Values'] + '')
                # tag_create_script += ' copy grants'

                alter_statement = 'alter tag ' + full_tag_name + ' set comment = $$' + constants.OBJECT_CREATION_COMMENT + '$$'

                # Create the darn thing...
                try:
                    result = st.session_state['main_session'].sql(tag_create_script).collect()
                    st.session_state['main_session'].sql(alter_statement).collect()
                except SnowparkSQLException as err:
                    st.error(err)
                    st.code(tag_create_script, language='sql')
                    if alter_statement:
                        st.code(alter_statement, language='sql')
                    submitted = False

                if submitted:
                    st.success(result[0]['status'])
                    st.experimental_memo.clear()
                    time.sleep(2)
                    st.experimental_rerun()

    return True

def fetch_and_display_tags():

    # tags_df = pd.DataFrame()
    # if st.session_state['authenticated']:

    # Get available tags
    tags_df = pd.DataFrame(sesh.cache_sql_memory('show tags in account', st.session_state['account'], st.session_state['current_context']['role']))

    # Filter out the Snowflake system tags
    tags_df = tags_df[~tags_df['database_name'].isin(['SNOWFLAKE'])]

    tag_values_sql = '''-- Tag values query
    select tag_database
          ,tag_schema
          ,tag_name
          ,array_agg(distinct tag_value) as tag_values
      from snowflake.account_usage.tag_references
     group by 1, 2, 3
    '''
    
    tag_values_df = pd.DataFrame(sesh.cache_sql_memory(tag_values_sql, st.session_state['account'], st.session_state['current_context']['role']))

    # Initialize columns
    for default_col in ['TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME', 'TAG_VALUES']:
        if default_col not in tag_values_df:
            tag_values_df[default_col] = []

    tags_df = tags_df.merge(tag_values_df, how='left', left_on=['database_name', 'schema_name', 'name'], right_on=['TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME'])
    tags_df['TAG_VALUES'] = tags_df['TAG_VALUES'].fillna('[]')
    tags_df['allowed_values'] = tags_df['allowed_values'].fillna('[]')

    tag_search_val = st.text_input('Search Tags', value='', placeholder='search value')
    if tag_search_val:
        tags_df = tags_df[tags_df.apply(lambda row: row.astype(str).str.contains(tag_search_val, case=False).any(), axis=1)]

    rolled_up_tags = {}
    for index, row in tags_df.iterrows():
        db = row['database_name']
        schema = row['schema_name']
        tag = row['name']
        values = json.loads(row['TAG_VALUES'])
        allowed_vals = json.loads(row['allowed_values'])

        if db not in rolled_up_tags:
            rolled_up_tags[db] = {}
        if schema not in rolled_up_tags[db]:
            rolled_up_tags[db][schema] = []

        tag_dict = {
            'name': tag,
            'values': values,
            'allowed_values': allowed_vals
        }
        rolled_up_tags[db][schema].append(tag_dict)

    unique_dbs = len(pd.unique(tags_df['database_name']))
    default_expanded = True 
    if unique_dbs > 1:
        default_expanded = False

    for db in rolled_up_tags.keys():
        for schema in rolled_up_tags[db]:
            expander_label = db + ' > ' + schema + ' (' + str(len(rolled_up_tags[db][schema])) + ')'
            with st.expander(expander_label, expanded=default_expanded):
                for tag in rolled_up_tags[db][schema]:
                    # st.json(tag)

                    tag_col1, tag_col2 = st.columns(2)
                    with tag_col1:
                        # st.write('Tag Name')
                        st.code('' + tag['name'] + '', language='sass')

                    with tag_col2:
                        # st.caption('Allowed Values: ' + json.dumps(tag['allowed_values']))
                        st.code('Allowed Values: ' + json.dumps(tag['allowed_values']), language='inform7')
                        st.code('Current Values: ' + json.dumps(tag['values']), language='arduino')

                    with tag_col1:
                        drop_button_key = db + '.' + schema + '.' + tag['name']
                        # drop_reveal = st.radio(
                        #     'Show Drop Button', 
                        #     ['Hide', 'Show'], 
                        #     key=drop_button_key + '_radio', 
                        #     help='Expose a button which can drop the tag forever.', 
                        #     label_visibility='visible', 
                        #     horizontal=True
                        # )
                        drop_reveal = st.checkbox(
                            'Show Drop Button',
                            value=False,
                            key=drop_button_key + '_checkbox', 
                            help='Expose a button which can drop the tag forever.', 
                            label_visibility='visible', 
                            # horizontal=True
                        )

                        if drop_reveal: # == 'Show':
                            if st.button('Drop Tag', key=drop_button_key + '_drop_button', help="When you click this, it's gone. Don't click this accidentally..."):
                                drop_result = sesh.run_sql('drop tag ' + drop_button_key)
                                st.success(drop_result[0]['status'])
                                sesh.clear_all_cache()
                                time.sleep(2)
                                st.experimental_rerun()

                    st.markdown('---')
    return True

def main():
    st.markdown('### Tag Management')
    context = {}

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

    if st.session_state['authenticated']:
        sesh.initialize_assistant_db(schema='tagging')
        with st.expander('Create Tag'):
            tag_creation_form(display_format='centered', label_visibility_setting='collapsed', show_form_title_text=False)

        fetch_and_display_tags()

    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Tag Management',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    main()