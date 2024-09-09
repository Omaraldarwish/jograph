import networkx as nx
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go

from utils.graph import get_person_influence

st.set_page_config(layout="wide")
st.title('Elections Graph Search | Simple Relative Search')
st.markdown("<hr>", unsafe_allow_html=True)

with st.sidebar:
    selected_national_no = st.text_input('National Number', value='')
    
    RELATIONSHIPS = ['FATHER', 'MOTHER', 'SPOUSE', 'SIBLING']
    selected_relationships = st.multiselect('Relationship', RELATIONSHIPS, default=RELATIONSHIPS, format_func=lambda x: x.title())
    if len(selected_relationships) == 0:
        st.error('Please select at least one relationship.')
        st.stop()

    selected_degree = st.slider('Degree', min_value=1, max_value=5, value=3)

query_filters = {
        'national_no': selected_national_no,
        'relationship': selected_relationships,
        'degree': selected_degree
    }

graph = get_person_influence(query_filters)
pos = nx.nx_agraph.graphviz_layout(graph, prog='sfdp')

edge_x = []
edge_y = []
for edge in graph.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.append(x0)
    edge_x.append(x1)
    edge_x.append(None)
    edge_y.append(y0)
    edge_y.append(y1)
    edge_y.append(None)

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=2, color='#888'),
    hoverinfo='none',
    mode='lines')

hover_vars = {
    'Person':['full_name', 'national_no', 'primary_key', 'dob'],
}
node_x = []
node_y = []
node_text = []
hover_text = []
colors = []
for node in graph.nodes():
    _node = graph.nodes[node]
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)
    node_text.append(_node.get('display'))
    colors.append(_node.get('color'))
    hover_text.append(f"<br>".join([f"{k}: {_node.get(k)}" for k in hover_vars.get(_node.get('label'), ['display'])]))

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers+text',
    hoverinfo='text',
    hovertext=hover_text,
    text=node_text,
    textposition="top center",
    # fillcolor=colors,
    marker=dict(
        showscale=False,
        size=40,
        line_width=2,
        color=colors,
    ),
    )

fig = go.Figure(data=[edge_trace, node_trace],
                layout=go.Layout(
                    titlefont_size=16,
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=0,l=0,r=0,t=0),
                    xaxis=dict(showgrid=False, zeroline=False),
                    yaxis=dict(showgrid=False, zeroline=False))
                )

st.plotly_chart(fig, use_container_width=True)