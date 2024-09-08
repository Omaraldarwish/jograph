from multiprocessing.managers import ValueProxy
from turtle import circle
from polars import col
import streamlit as st

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

st.write(get_person_influence(query_filters))