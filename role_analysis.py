import pandas as pd
import streamlit as st
import st_snowpark_session as sesh
import networkx as nx
# import netgraph
# Might be worth trying to implement this at some point: 
# https://stackoverflow.com/questions/39801880/how-to-use-the-pos-argument-in-networkx-to-create-a-flowchart-style-graph

def main():
    # Just make sure we are authenticated...
    if 'authenticated' not in st.session_state or st.session_state['authenticated'] == False:
        return False 
    # Ok now we can do stuff...

    # Check the current role.
    current_role = st.session_state['main_session'].get_current_role().replace('"', '')

    if current_role not in ['ACCOUNTADMIN', 'SECURITYADMIN']:
        st.warning('''WARNING: Thorough role analysis requires using either `ACCOUNTADMIN` or `SECURITYADMIN` role. 
        Please change role using the context selector or relog in with an appropriate role.''')
    else:
        with st.spinner('Fetching roles...'):
            all_available_roles_df = pd.DataFrame(sesh.cache_sql_disk('show roles', st.session_state['account'], st.session_state['current_context']['role']))

        with st.spinner('Analyzing role assignment and hierarchy...'):
            role_analysis = analyze_roles(all_available_roles_df)

        st.table(all_available_roles_df)

    return True

# @st.cache_data(show_spinner=False)
def analyze_roles(roles_dataframe=None, users_dataframe=None):
    hierarchy_graph = nx.MultiDiGraph()
    # hierarchy_graph = nx.DiGraph()
    hierarchy_dict = {}

    # Must be the right type...
    if roles_dataframe is None:
        return hierarchy_dict

    # Must have at least one row...
    if len(roles_dataframe.index) == 0:
        return hierarchy_dict

    # Ok, let's iterate like a mad person
    for index, row in roles_dataframe.iterrows():
        hierarchy_graph.add_node(row['name'], object_type='role')

    # Check the users dataframe and add users if we feel like it... 
    if users_dataframe is not None and len(users_dataframe.index) > 0:
        for index, row in users_dataframe.iterrows():
            hierarchy_graph.add_node(row['name'], object_type='user')

    # All the nodes have been added. Let's connect them... 
    for index, row in roles_dataframe.iterrows():
        role_grants_df = sesh.cache_sql_memory('show grants of role ' + row['name'])
        for grant in role_grants_df:
            if grant['granted_to'] == 'USER' and users_dataframe is None:
                # This is a user and users are not enabled, skip this loop.
                continue 
            else:
                hierarchy_graph.add_edge(row['name'], grant['grantee_name'])

    st.write(hierarchy_graph.edges())
    # st.write(nx.draw_circular(hierarchy_graph))
    # st.write(nx.nx_agraph.graphviz_layout(hierarchy_graph))
    # st.write(nx.draw_networkx_nodes(hierarchy_graph, pos=))
    # st.pyplot(fig=netgraph.Graph(hierarchy_graph))

    return hierarchy_dict

if __name__ == '__main__':
    st.set_page_config(
        page_title='Role Analysis',
        layout='centered',
        initial_sidebar_state='collapsed',
    )

    st.markdown('### Role Analysis')

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

        main()
