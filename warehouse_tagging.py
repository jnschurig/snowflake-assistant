import pandas as pd
import streamlit as st
import st_snowpark_session as sesh
import reference_queries as rq
import json 
# import time

def format_tag_dropdown(available_tags_df):
    tags_list = ['New Tag']
    if type(available_tags_df) == None:
        available_tags_df = pd.DataFrame()
        available_tags_df.columns = ['database_name', 'schema_name', 'name', 'allowed_values']

    for idx, row in available_tags_df.iterrows():
        tag_item = row['database_name'].lower() + '.' + row['schema_name'].lower() + '.' + row['name']
        tags_list.append(tag_item)

    return tags_list

def display_warehouse_tags(warehouse_details, tags_df=None):
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

    tag_values_df = tag_values_df.merge(tags_df, how='left', left_on=['tag_db', 'tag_schema', 'tag_name'], right_on=['database_name', 'schema_name', 'name'])

    # st.markdown('##### ' + warehouse_details['name'] + ' (' + warehouse_details['size'] + ')')
    with st.expander(warehouse_details['name'] + ' (' + warehouse_details['size'] + ')'):
        # st.write(warehouse_details['name'] + ' (' + warehouse_details['size'] + ')')
        st.caption('*New tag values may take up to 3 hours to propagate in the Snowflake account usage views.')

        for index, row in tag_values_df.iterrows():
            key_base = row['tag_db'] + '.' + row['tag_schema'] + '.' + row['tag_name']
            tag_disp_col1, tag_disp_col2 = st.columns(2)

            with tag_disp_col1:
                st.markdown('`' + row['tag_db'] + ' > ' + row['tag_schema'] + '`\n\n' + row['tag_name'])

            with tag_disp_col2:
                new_value = st.text_input('Tag Value', row['value'], key=key_base + '_new_val_' + str(index), label_visibility='collapsed',)
                if new_value and new_value != row['value']:
                    alter_text = 'alter warehouse ' + warehouse_details['name'] + ' set tag ' + key_base + ' = $$' + new_value + '$$'
                    try:
                        # add_tag_value_result = sesh.run_sql(alter_text)
                        add_tag_value_result = st.session_state['main_session'].sql(alter_text).collect()
                        st.success('Successfully updated tag value!')
                    except:
                        st.error('Error: Unable to set tag value. Check to make sure the current role has access to set tags on this warehouse.')
                    # time.sleep(2)
                    # st.experimental_rerun()
                    # new_value = row['value']
                st.caption('Allowed Values: ' + row['allowed_values'])
            
        # add_tag_form(tag_values_df)
        with st.form(key=warehouse_details['name'] + '_new_value_form', clear_on_submit=True):
            dropdown_values = format_tag_dropdown(tags_df)
            new_tag_val_col1, new_tag_val_col2 = st.columns(2)

            with new_tag_val_col1:
                new_tag_name = st.selectbox('Tag', dropdown_values, label_visibility='collapsed')

            with new_tag_val_col2:
                new_tag_value = st.text_input('Tag Value', '', placeholder='Tag Value', label_visibility='collapsed')

            if st.form_submit_button('Set Tag'):
                if new_tag_value and new_tag_name != 'New Tag':
                    new_tag_alter = 'alter warehouse ' + warehouse_details['name'] + ' set tag ' + new_tag_name + ' = $$' + new_tag_value + '$$'
                    try:
                        # new_tag_result = sesh.run_sql(new_tag_alter)
                        new_tag_result = st.session_state['main_session'].sql(new_tag_alter).collect()
                        st.success(new_tag_result[0]['status'])
                    except:
                        st.error('Error: Unable to set tag value. Check to make sure the current role has access to set tags on this warehouse.')
                    # st.code(new_tag_alter)
                    # time.sleep(2)
                    # st.experimental_rerun()
                    new_tag_value = 'New Tag'
                else:
                    st.error('Error: Be sure to select a tag AND set a tag value. If no tags are available, create one...')

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
            # summary_days = st.slider('Days of Recent History', 0, 30, 7, step=1, help="0 indicates today's history only")
            only_untagged = st.checkbox('Untagged Only', help='Only show warehouses that are not tagged at all.')

        with wh_option_col2:
            wh_search_val = st.text_input('Warehouse Search', value='', placeholder='Warehouse Search', label_visibility='collapsed')
        # if 'role'
        warehouse_df = pd.DataFrame(sesh.cache_sql_disk('show warehouses', st.session_state['account'], context['role']))
        # wh_summary_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_QUERY_HISTORY_SUMMARY.format(date_range = str(summary_days)), st.session_state['account'], context['role']))
        wh_tags_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_TAG_SUMMARY, st.session_state['account'], context['role']))
        # wh_metering_df = pd.DataFrame(sesh.cache_sql_memory(rq.WAREHOUSE_METERING_SUMMARY.format(date_range = str(summary_days)), st.session_state['account'], context['role']))

        # warehouse_df = warehouse_df.merge(wh_summary_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')
        warehouse_df = warehouse_df.merge(wh_tags_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')
        # warehouse_df = warehouse_df.merge(wh_metering_df, how='left', left_on='name', right_on='WAREHOUSE_NAME')

        warehouse_df['TAG_JSON'] = warehouse_df['TAG_JSON'].fillna('[]')

        if only_untagged:
            warehouse_df = warehouse_df[warehouse_df['TAG_JSON'].isin(['[]'])]

        if wh_search_val:
            warehouse_df = warehouse_df[warehouse_df.apply(lambda row: row.astype(str).str.contains(wh_search_val, case=False).any(), axis=1)]

        # with wh_option_col2:
        wh_filter_list = st.multiselect('Select Warehouse', warehouse_df['name'], default=None, label_visibility='collapsed')

        if wh_filter_list:
            warehouse_df = warehouse_df[warehouse_df['name'].isin(wh_filter_list)]

        account_tags_df = pd.DataFrame(sesh.cache_sql_memory('show tags in account', st.session_state['account'], context['role']))
        account_tags_df = account_tags_df[~account_tags_df['database_name'].isin(['SNOWFLAKE'])]
        account_tags_df['allowed_values'] = account_tags_df['allowed_values'].fillna('[]')


        # display_warehouse_summary(warehouse_df)
        for index, row in warehouse_df.iterrows():
            display_warehouse_tags(row, account_tags_df)

        # st.table(account_tags_df)


    # st.table(warehouse_df)

    # st.json(st.session_state)
    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Warehouse Tagging',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    main()