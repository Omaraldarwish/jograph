from turtle import circle
from polars import col
import streamlit as st

from utils.graph import get_circles, get_centers, get_boxes, get_family_counts

st.set_page_config(layout="wide")
st.title('Elections Graph Search')
st.markdown("<hr>", unsafe_allow_html=True)

with st.sidebar:    
    _tmp = get_circles()
    _values = [x['circle_id'] for x in _tmp]
    _format = {x['circle_id']: x['circle_name'] for x in _tmp}
    selected_circle = st.selectbox('Circle', options=_values, format_func=lambda x: _format[x])
    
    if not selected_circle:
        st.stop()
    
    col1, col2 = st.columns([1, 2], vertical_alignment='center')

    with col1:
        filter_center = st.checkbox('Filter by Center')
    with col2:
        _tmp = get_centers(selected_circle)
        _values = [x['center_id'] for x in _tmp]
        _format = {x['center_id']: x['center_name'] for x in _tmp}

        selected_center = st.selectbox('Center', options=_values, format_func=lambda x: _format[x], disabled=not filter_center)

    col1, col2 = st.columns([1, 2], vertical_alignment='center')

    with col1:
        filter_box = st.checkbox('Filter by Box')
    
    with col2:
    
        _tmp = get_boxes(selected_circle, selected_center)
        _values = [x['box_id'] for x in _tmp]
        _format = {x['box_id']: x['box_name'] for x in _tmp}
        selected_box = st.selectbox('Box', options=_values, format_func=lambda x: _format[x], disabled=not filter_box)

    st.markdown("<hr>", unsafe_allow_html=True)

    RELATIONSHIPS = ['FATHER', 'MOTHER', 'SPOUSE', 'SIBLING']
    selected_relationships = st.multiselect('Relationship', RELATIONSHIPS, default=RELATIONSHIPS, format_func=lambda x: x.title())
    if len(selected_relationships) == 0:
        st.error('Please select at least one relationship.')
        st.stop()

    selected_degree = st.slider('Degree', min_value=1, max_value=5, value=3)

query_filters = {
        'circle': selected_circle,
        'center': selected_center if filter_center else None,
        'box': selected_box if filter_box else None,
        'relationship': selected_relationships,
        'degree': selected_degree
    }

st.write(query_filters)

with st.sidebar:    
    if st.button('Search'):
        with st.spinner('Searching...'):
            data = get_family_counts(query_filters)    

st.dataframe(get_family_counts(query_filters))