import pandas as pd
import streamlit as st
import st_snowpark_session as sesh
import reference_queries as rq
import json 
import utility
# import time

def format_tag_dropdown(available_tags_df, qualify_names=True):
    ''' 
    Takes the tags dataframe (with database, schema, and tag name) 
    and returns a list object with formatted names of them all. If 
    the 'qualify_names' setting is set to True, the name will include 
    the database and schema of the tag. A dummy '<Tag>' is added as 
    the first tag in the list.
    '''
    tags_list = ['<Tag>']
    if type(available_tags_df) == None:
        available_tags_df = pd.DataFrame()
        available_tags_df.columns = ['database_name', 'schema_name', 'name', 'allowed_values']

    for idx, row in available_tags_df.iterrows():
        if qualify_names:
            tag_item = row['database_name'].lower() + '.' + row['schema_name'].lower() + '.' + row['name']
        else:
            tag_item = row['name']
        tags_list.append(tag_item)

    return tags_list

def display_warehouse_tags(warehouse_details, tags_df=None, expanders_open=False):
    ''' 
    Function for displaying a warehouse's details. Takes as inputs a row of warehouse 
    details, the tag information dataframe, and a bool for if the expanders should 
    be open or closed by default.
    '''
    # Check inputs and correct as necessary
    # Warehouse details should be a dict or "<class 'pandas.core.series.Series'>"
    if type(tags_df) == None:
        tags_df = pd.DataFrame()
        tags_df.columns = ['database_name', 'schema_name', 'name', 'allowed_values']

    tag_dict = []
    if 'TAG_JSON' in warehouse_details:
        if type(warehouse_details) is dict:
            tag_dict = warehouse_details['TAG_JSON']
        if str(type(warehouse_details)) == "<class 'pandas.core.series.Series'>":
            tag_dict = json.loads(warehouse_details['TAG_JSON'])

    tag_values_df = pd.DataFrame.from_dict(tag_dict, orient='columns')#, convert_axes=True, typ='frame')
    if len(tag_values_df.index) == 0:
        tag_values_df = pd.DataFrame(columns=['tag_db', 'tag_schema', 'tag_name'])

    # Join the reference tags to incoming row of warehouse tags
    tag_values_df = tag_values_df.merge(tags_df, how='left', left_on=['tag_db', 'tag_schema', 'tag_name'], right_on=['database_name', 'schema_name', 'name'])

    # Gotta take care of dumb NaN values.
    tag_values_df['allowed_values'] = tag_values_df['allowed_values'].fillna('[]')

    # Create an expander and display the warehouse info.
    with st.expander(warehouse_details['name'] + ' (' + warehouse_details['size'] + ')', expanded=expanders_open):
        st.caption('*Recently assigned tag values may take up to 3 hours to propagate in the Snowflake account usage views.')

        # Show values of each tag assigned to this warehouse. 
        for index, row in tag_values_df.iterrows():
            key_base = row['tag_db'] + '.' + row['tag_schema'] + '.' + row['tag_name']

            # In the future we should add a third column with an "UN SET" button to clear the tag value.
            tag_disp_col1, tag_disp_col2 = st.columns(2)

            with tag_disp_col1:
                # Fully qualify the name of each tag.
                st.markdown('`' + row['tag_db'] + ' > ' + row['tag_schema'] + '`\n\n' + row['tag_name'])

            with tag_disp_col2:
                # Display the existing tag value and accept a new value for "updating" the tag.
                new_value = st.text_input('Tag Value', row['value'], key=key_base + '_new_val+' + warehouse_details['name'], label_visibility='collapsed',)
                st.caption('Allowed Values: ' + str(row['allowed_values']))
            if new_value and new_value != row['value']:
                alter_text = 'alter warehouse ' + warehouse_details['name'] + ' set tag ' + key_base + ' = $$' + new_value + '$$'
                try:
                    add_tag_value_result = st.session_state['main_session'].sql(alter_text).collect()
                    st.success('Successfully updated tag value!')
                except:
                    st.error('Error: Unable to set tag value. Check to make sure the current role has access to set tags on this warehouse.')
            
        # Add a form for adding new tags and setting values.
        with st.form(key=warehouse_details['name'] + '_new_value_form', clear_on_submit=True):
            # Select a tag from the dropdown.
            dropdown_values = format_tag_dropdown(tags_df)

            # Display form in columns
            new_tag_val_col1, new_tag_val_col2 = st.columns(2)

            with new_tag_val_col1:
                new_tag_name = st.selectbox('Tag', dropdown_values, label_visibility='collapsed')

            with new_tag_val_col2:
                new_tag_value = st.text_input('Tag Value', '', placeholder='Tag Value', label_visibility='collapsed')

            # This was in a third column, but it made the fields too narrow to show the name of the tag.
            tag_submitted = st.form_submit_button('Set Tag')

            # Assign the new value.
            if tag_submitted:
                if new_tag_value and new_tag_name != '<Tag>':
                    new_tag_alter = 'alter warehouse ' + warehouse_details['name'] + ' set tag ' + new_tag_name + ' = $$' + new_tag_value + '$$'
                    try:
                        new_tag_result = st.session_state['main_session'].sql(new_tag_alter).collect()
                        # st.success(new_tag_result[0]['status'])
                        st.success('New tag value added successfully!')
                    except:
                        st.error('Error: Unable to set tag value. Check to make sure the current role has access to set tags on this warehouse.')
                    new_tag_value = '<Tag>'
                else:
                    st.error('Error: Be sure to select a tag AND set a tag value. If no tags are available, create one...')

    return True

def main():
    ''' 
    The main workflow for warehouse tagging. Takes no inputs, but expects 
    st.session_state to contain 'authenticated' and 'main_session', as a 
    bool and Snowflake connection object.
    '''
    st.markdown('### Warehouse Tagging')

    warehouse_df = pd.DataFrame()

    # Authentication section
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if not st.session_state['authenticated']:
        sesh.display_authentication()
        if st.session_state['authenticated']:
            st.experimental_rerun()

    if st.session_state['authenticated']:
        # Begin the main section by displaying context
        context_container = st.container()
        with st.sidebar:
            st.markdown('#### Context Selection')
            context = sesh.manage_context('narrow')
        with context_container:
            st.caption(sesh.format_context(context))

        # Options and search columns
        wh_option_col1, wh_option_col2 = st.columns([1, 3])

        with wh_option_col1:
            all_expanders_open = st.checkbox('Expand All', value=True, help='Expand all warehouse tag details.')
            only_untagged = st.checkbox('Untagged Only', help='Only show warehouses that are not tagged at all.')

        with wh_option_col2:
            wh_search_val = st.text_input('Search', value='', placeholder='Warehouse Search', label_visibility='collapsed', help='Search among warehouses, their attributes, tag names, and tag values.')

        # Fetch all warehouse information available to the user/role.
        warehouse_df = pd.DataFrame(sesh.cache_sql_disk('show warehouses', st.session_state['account'], context['role']))

        # Fetch all existing warehouse tag values.
        wh_tags_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_TAG_SUMMARY, st.session_state['account'], context['role']))

        # Fetch all tags in the account, regardless of current assignments.
        account_tags_df = pd.DataFrame(sesh.cache_sql_memory('show tags in account', st.session_state['account'], context['role']))

        # Gather search and filter options.
        wh_filter_list = st.multiselect('Select Warehouse', warehouse_df['name'], default=None, label_visibility='collapsed')

        tag_name_search_col, tag_value_search_col = st.columns(2)
        with tag_name_search_col:
            # Explicitly search tag NAMES
            tag_name_search_val = st.text_input('Search Tag Names', value='', placeholder='Search Tag Names', label_visibility='collapsed')
        
        with tag_value_search_col:
            # Explicitly search tag VALUES
            tag_value_search_val = st.text_input('Search Tag Values', value='', placeholder='Search Tag Values', label_visibility='collapsed')

        # Join Warehouse information and tag information
        warehouse_df = warehouse_df.merge(wh_tags_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')

        # NA and NaN are the dumbest values
        warehouse_df['TAG_JSON'] = warehouse_df['TAG_JSON'].fillna('[]')
        account_tags_df = account_tags_df[~account_tags_df['database_name'].isin(['SNOWFLAKE'])]

        # For each warehouse, get valid tag names (as keys) and tag values as separate lists (for searching)
        warehouse_df = utility.split_dataframe_column_json(warehouse_df, 'TAG_JSON')

        # Apply filters and searches from above and reduce size of joined dataframes
        if only_untagged:
            warehouse_df = warehouse_df[warehouse_df['TAG_JSON'].isin(['[]'])]

        if wh_search_val:
            warehouse_df = warehouse_df[warehouse_df.apply(lambda row: row.astype(str).str.contains(wh_search_val, case=False).any(), axis=1)]

        if wh_filter_list:
            warehouse_df = warehouse_df[warehouse_df['name'].isin(wh_filter_list)]

        if tag_name_search_val:
            warehouse_df = warehouse_df[warehouse_df['TAG_JSON_keys'].str.contains(tag_name_search_val, case=False)]

        if tag_value_search_val:
            warehouse_df = warehouse_df[warehouse_df['TAG_JSON_values'].str.contains(tag_value_search_val, case=False)]

        # Iterate through each row of warehouse info and display details.
        for index, row in warehouse_df.iterrows():
            display_warehouse_tags(row, account_tags_df, all_expanders_open)

    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Warehouse Tagging',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    main()