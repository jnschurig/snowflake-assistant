import pandas as pd
import streamlit as st
import st_snowpark_session as sesh

def main():
    st.markdown('### Warehouse Tagging')

    warehouse_df = pd.DataFrame()

    if st.session_state['authenticated']:
        context = st.session_state['current_context']
        st.caption(sesh.format_context(context))
        # if 'role'
        warehouse_df = pd.DataFrame(sesh.cache_sql_disk('show warehouses', st.session_state['account'], context['role']))



    st.table(warehouse_df)

    st.json(st.session_state)
    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Warehouse Tagging',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    sesh.main_handler('collapsed')

    main()