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

if not selected_national_no:
    st.stop()

query_filters = {
        'national_no': selected_national_no,
        'relationship': selected_relationships,
        'degree': selected_degree
    }

data, data2 = get_person_influence(query_filters)

col1, col2, col3 = st.columns([1, 1, 1])
col1.metric(label='Total Relatives', value=data['num_relatives'].sum())
col2.metric(label='Total Circles', value= data['circle'].nunique())
col3.metric(label='Total Centers', value=data['center'].nunique())

st.markdown("<hr>", unsafe_allow_html=True)

df_barplot_circles = data.groupby('circle')['num_relatives'].sum().reset_index().sort_values('num_relatives', ascending=False)
barplot_circles = px.bar(df_barplot_circles, x='circle', y='num_relatives', title='Total Relatives by Circle')

df_barplot_centers = data.groupby(['circle', 'center'])['num_relatives'].sum().reset_index().sort_values('num_relatives', ascending=False)
barplot_centers = px.bar(df_barplot_centers, x='center', y='num_relatives', title='Total Relatives by Center')

st.plotly_chart(barplot_circles, use_container_width=True)
st.plotly_chart(barplot_centers, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)
st.write('Center and Circle Details')
st.dataframe(data, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)
st.write('Drill Down')
st.dataframe(data2, use_container_width=True)
# --------------------------------------------------------------------------------------------------
# pos = nx.multipartite_layout(graph, subset_key='pos_label', align='horizontal', scale=2)
# edge_x = []
# edge_y = []
# for edge in graph.edges():
#     x0, y0 = pos[edge[0]]
#     x1, y1 = pos[edge[1]]
#     edge_x.append(x0)
#     edge_x.append(x1)
#     edge_x.append(None)
#     edge_y.append(y0)
#     edge_y.append(y1)
#     edge_y.append(None)

# edge_trace = go.Scatter(
#     x=edge_x, y=edge_y,
#     line=dict(width=2, color='#888'),
#     hoverinfo='none',
#     mode='lines')

# person_hover_vars = ['full_name', 'national_no', 'primary_key', 'dob']
# node_x = []
# node_y = []
# node_text = []
# hover_text = []
# colors = []
# for node in graph.nodes():
#     _node = graph.nodes[node]
#     _label = _node.get('label')
#     x, y = pos[node]
#     node_x.append(x)
#     node_y.append(y)
#     node_text.append(_node.get('display'))
#     colors.append(_node.get('color'))

#     if _label == 'Person':
#         hover_text.append(f"<br>".join([f"{k}: {_node.get(k)}" for k in person_hover_vars]))
#     else:
#         hover_text.append(f"{'name'}: {_node.get('display')}")

# node_trace = go.Scatter(
#     x=node_x, y=node_y,
#     mode='markers+text',
#     hoverinfo='text',
#     hovertext=hover_text,
#     text=node_text,
#     textposition="top center",
#     marker=dict(
#         showscale=False,
#         size=30,
#         line_width=2,
#         color=colors,
#     ),
#     )

# fig = go.Figure(data=[edge_trace, node_trace],
#                 layout=go.Layout(
#                     titlefont_size=16,
#                     showlegend=False,
#                     hovermode='closest',
#                     margin=dict(b=0,l=0,r=0,t=0),
#                     xaxis=dict(showgrid=False, zeroline=False),
#                     yaxis=dict(showgrid=False, zeroline=False),
#                 ),
#                 )

# st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------------------------------------
