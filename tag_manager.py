import pandas as pd
import streamlit as st
import st_snowpark_session as sesh 
import constants
import time
from snowflake.snowpark.exceptions import SnowparkSQLException

def tag_creation_form(display_format='centered', label_visibility_setting='visible'):
            
    with st.form(key='tag_creation_form', clear_on_submit=True):
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
        # st.caption('**Use __Allowed Values__ field with caution.')
                
        submitted = st.form_submit_button('Create Tag')
        if submitted:
            if not new_tag_dict['Tag Name']:
                st.error('Tag Name is required!')
                submitted = False 
                time.sleep(3)
                st.experimental_rerun()
            else:
                # Assemble the creation script...
                tag_create_script = 'create or replace tag '
                if new_tag_dict['Tag Database']:
                    tag_create_script += str(new_tag_dict['Tag Database']) + '.'
                if new_tag_dict['Tag Schema']:
                    tag_create_script += str(new_tag_dict['Tag Schema']) + '.'
                tag_create_script += new_tag_dict['Tag Name']
                tag_create_script += ' comment = $$' + constants.OBJECT_CREATION_COMMENT + '$$'
                # tag_create_script += ' copy grants'

                # Create the darn thing...
                try:
                    result = st.session_state['main_session'].sql(tag_create_script).collect()
                except SnowparkSQLException as err:
                    st.error(err)
                    st.code(tag_create_script, language='sql')
                    submitted = False

                if submitted:
                    st.success(result[0]['status'])
                    st.experimental_memo.clear()
                    time.sleep(3)
                    st.experimental_rerun()

    return 

def fetch_and_display_tags():

    tags_df = pd.DataFrame()
    if st.session_state['authenticated']:
        sesh.initialize_assistant_db(schema='tagging')
        tag_creation_form(display_format='centered', label_visibility_setting='collapsed')

        # Get available tags
        tags_df = pd.DataFrame(sesh.cache_sql_memory('show tags in account'))

        # Filter out the Snowflake system tags
        tags_df = tags_df[~tags_df['database_name'].isin(['SNOWFLAKE'])]




    # Create new tag using local context (or specified context...)

    # Offer to initialize assistant database / tagging schema and set context
    # We have all the tags (that the role has access to)
    # Let's begin adding new tags and reviewing the values.

    # Search tags and filter
    st.table(tags_df)


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

    fetch_and_display_tags()

    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Tag Management',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    main()