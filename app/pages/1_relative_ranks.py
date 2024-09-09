import streamlit as st

from utils.graph import get_circles, get_centers, get_boxes, get_relative_counts, get_counts_by_location

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

    selected_degree = st.slider('Degree', min_value=1, max_value=3, value=2)

query_filters = {
        'circle': selected_circle,
        'center': selected_center if filter_center else None,
        'box': selected_box if filter_box else None,
        'relationship': selected_relationships,
        'degree': selected_degree
    }

counts = get_counts_by_location(query_filters)

col1, col2, col3 = st.columns([1, 1, 1])
col1.metric(label='Total Centers', value=counts['num_centers'])
col2.metric(label='Total Boxes', value=counts['num_boxes'])
col3.metric(label='Total Voters', value=f"{counts['num_voters']:000,}")

with st.sidebar:
    search_trigger = False    
    if st.button('Search'):
        search_trigger = True
        with st.spinner('Searching...'):
            q, data = get_relative_counts(query_filters)    
    
if search_trigger:
    # format data
    data = (
        data
        .assign(
            influence_perc = lambda x: x['num_relatives'] / counts['num_voters'] * 100,
        )
        .pipe(lambda x: x[['num_relatives', 'influence_perc'] + [c for c in x.columns if c not in ['num_relatives', 'influence_perc']]])
    )
    st.write(data)

    family_value_counts = data['family_name'].value_counts()
    
else:
    st.write('Click on Search to get the results.')

if search_trigger:
    st.markdown("<hr>", unsafe_allow_html=True)
    with st.expander("See Debug Info:"):    
        st.write(
            {
                'query_filters': query_filters,
                'counts': counts,
                'query_string': "\n".join([l.strip() for l in q.splitlines()]),
            }
        )