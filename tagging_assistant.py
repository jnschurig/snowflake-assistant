import streamlit as st
import warehouse_tagging as wt
import tag_manager as tm 
import st_snowpark_session as sesh

def go():
    st.markdown('#### Tagging Assistant')

    context_container = st.container()

    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    home, tag_tab, warehouse_tab  = st.tabs(['Home', 'Tag Management', 'Warehouse Tagging'])

    with home:
        st.markdown('''
        Welcome to the Tagging Assistant. 
        
        - Create and drop tags in the Tag Management tab. 
        - Use the object tagging tabs to apply tags and values.
        - Enjoy the context section here and don't forget to use the appropriate role for tagging in your environment.

        Happy Tagging!
        ''')

        if not st.session_state['authenticated']:
            sesh.display_authentication()
            if st.session_state['authenticated']:
                st.experimental_rerun()

    if st.session_state['authenticated']:

        with st.expander('Change Context', expanded=True):
            context = sesh.manage_context('centered')
        context_container.caption(sesh.format_context(context))

        with tag_tab:
            with st.expander('Create Tag'):
                tm.tag_creation_form(display_format='centered', label_visibility_setting='collapsed', show_form_title_text=False)
            tm.fetch_and_display_tags()

        with warehouse_tab:
            wt.main()
    else:
        message = 'Please Authenticate'
        with tag_tab:
            st.markdown(message)
        with warehouse_tab:
            st.markdown(message)
    return True

if __name__ == '__main__':
    st.set_page_config(
        page_title='Tagging Assistant',
        layout='centered',
        initial_sidebar_state='expanded',
        page_icon='🧊'
    )

    with st.sidebar:
        st.markdown('#### App Options')
        if st.button('Close Snowflake Session'):
            sesh.close_session()
        if st.button('Clear Cache'):
            sesh.clear_all_cache()
    go()