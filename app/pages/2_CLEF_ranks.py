import select
import streamlit as st

from utils.graph import get_circles, get_centers, get_boxes, run_clef

st.set_page_config(layout="wide")
st.title('Elections Graph Search | Relative Rank')
st.markdown("<hr>", unsafe_allow_html=True)

with st.sidebar:    
    _tmp = get_circles()
    _values = [x['circle_id'] for x in _tmp]
    _format = {x['circle_id']: x['circle_name'] for x in _tmp}
    selected_circle = st.selectbox('Circle', options=_values, format_func=lambda x: _format[x])
    
    if not selected_circle:
        st.stop()
    
    col1, col2 = st.columns([1, 2], vertical_alignment='center')
    _tmp = get_centers(selected_circle)
    _values = [x['center_id'] for x in _tmp]
    _format = {x['center_id']: x['center_name'] for x in _tmp}
    filter_center = col1.checkbox('Filter by Center', value=True)
    selected_center = col2.selectbox('Center', options=_values, format_func=lambda x: _format[x], disabled=not filter_center)

    

    col1, col2 = st.columns([1, 2], vertical_alignment='center')
    _tmp = get_boxes(selected_circle, selected_center)
    _values = [x['box_id'] for x in _tmp]
    _format = {x['box_id']: x['box_name'] for x in _tmp}
    filter_box = col1.checkbox('Filter by Box')
    selected_box = col2.selectbox('Box', options=_values, format_func=lambda x: _format[x], disabled=not filter_box)

    st.markdown("<hr>", unsafe_allow_html=True)

    RELATIONSHIPS = ['FATHER', 'MOTHER', 'SPOUSE', 'SIBLING']
    selected_relationships = st.multiselect('Relationship', RELATIONSHIPS, default=RELATIONSHIPS, format_func=lambda x: x.title())
    if len(selected_relationships) == 0:
        st.error('Please select at least one relationship.')
        st.stop()
    
    st.markdown("<hr>", unsafe_allow_html=True)

    selected_probability = st.number_input('Probability', min_value=0.0, max_value=1.0, value=0.1)
    selected_seedSetSize = st.number_input('Rank Set Size', min_value=1, max_value=100, value=10)
    selected_monteCarloSimulations = st.number_input('Number of Simulations', min_value=1, max_value=10000, value=1000)

query_filters = {
        'circle': selected_circle,
        'center': selected_center if filter_center else None,
        'box': selected_box if filter_box else None,
        'relationship': selected_relationships,
        'probability': selected_probability,
        'seedSetSize': selected_seedSetSize,
        'monteCarloSimulations': selected_monteCarloSimulations
    }

with st.sidebar:
    search_trigger = False    
    if st.button('Search'):
        search_trigger = True
        with st.spinner('Searching...'):
            q, data = run_clef(query_filters)    
    
if search_trigger:
    st.write(data)
else:
    st.write('Click on Search to get the results.')

if search_trigger:
    st.markdown("<hr>", unsafe_allow_html=True)
    with st.expander("See Debug Info:"):    
        st.write(
            {
                'query_filters': query_filters,
                'query_string': "\n".join([l.strip() for l in q.splitlines()]),
            }
        )
