import os
from uuid import uuid4
from dotenv import load_dotenv
import neo4j
from graphdatascience import GraphDataScience
import networkx as nx
import streamlit as st

import pandas as pd

DB_DIR = os.path.join(os.path.dirname(__file__), 'db')

# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

@st.cache_resource
def get_driver():
    load_dotenv()
    uri = os.getenv('NEO4J__URI')
    user = os.getenv('NEO4J__USER')
    password = os.getenv('NEO4J__PASSWORD')

    return neo4j.GraphDatabase.driver(uri, auth=(user, password))

def get_gds():
    load_dotenv()
    uri = os.getenv('NEO4J__URI')
    user = os.getenv('NEO4J__USER')
    password = os.getenv('NEO4J__PASSWORD')

    return GraphDataScience(uri, auth=(user, password))

def run_query(query, **kwargs):
    with get_driver().session() as session: #type: ignore
        return session.run(query, **kwargs).data() 

# --------------------------------------------------------------------------------------------------
@st.cache_data
def get_circles():
    q = """
        MATCH (c:Circle)
        RETURN c.name as circle_name, elementId(c) as circle_id
        ORDER BY circle_name ASCENDING
    """

    return run_query(q)

@st.cache_data
def get_centers(circle_id):
    q = f"""
        MATCH (c:Circle)--(ce:Center)
        WHERE elementId(c) = '{circle_id}'
        RETURN ce.name as center_name, elementId(ce) as center_id
    """
    print(q)
    return run_query(q)

@st.cache_data
def get_boxes(circle_id, center_id):
    q = f"""
        MATCH (c:Circle)--(ce:Center)--(b:Box)
        WHERE elementId(c) = '{circle_id}' AND elementId(ce) = '{center_id}'
        RETURN b.name as box_name, elementId(b) as box_id
    """

    return run_query(q)

def get_counts_by_location(filters):
    target_box = filters.get('box')
    target_center = filters.get('center')
    target_circle = filters.get('circle')

    if target_box:
        q = f"""
            MATCH (p:Person)--(b:Box)
            WHERE elementId(b) = '{target_box}'
            RETURN COUNT(DISTINCT p) as num_voters
        """
        num_centers = 1
        num_boxes = 1
        num_voters = run_query(q)[0]['num_voters']

    elif target_center:
        q = f"""
            MATCH (p:Person)--(b:Box)--(c:Center)
            WHERE elementId(c) = '{target_center}'
            RETURN COUNT(DISTINCT p) as num_voters, COUNT(DISTINCT b) as num_boxes
        """

        res = run_query(q)[0]
        num_centers = 1
        num_boxes = res['num_boxes']
        num_voters = res['num_voters']

    else:
        q = f"""
            MATCH (p:Person)--(b:Box)--(c:Center)--(ci:Circle)
            WHERE elementId(ci) = '{target_circle}'
            RETURN COUNT(DISTINCT p) as num_voters, COUNT(DISTINCT b) as num_boxes, COUNT(DISTINCT c) as num_centers
        """

        res = run_query(q)[0]
        num_centers = res['num_centers']
        num_boxes = res['num_boxes']
        num_voters = res['num_voters']

    return {
        'num_centers': num_centers,
        'num_boxes': num_boxes,
        'num_voters': num_voters
    }

def build_match_block_from_location(filters):
    target_box = filters.get('box')
    target_center = filters.get('center')
    target_circle = filters.get('circle')

    if target_box:
        return f"""
            MATCH (box:Box)
            WHERE elementId(box) = '{target_box}'
        """
    elif target_center:
        return f"""
            MATCH (box:Box)--(center:Center)
            WHERE elementId(center) = '{target_center}'
        """
    else:
        return f"""
            MATCH (box:Box)--(center:Center)--(circle:Circle)
            WHERE elementId(circle) = '{target_circle}'
        """


def get_relative_counts(filters):
    target_relationships = f"({'|'.join(filters.get('relationship'))})"
    target_degrees = filters.get('degree', '1')
    RETURN_BLOCK = """
        RETURN
            person.first_name + ' ' + person.father_name + ' ' + person.grand_name + ' ' + person.family_name as full_name,
            // person.first_name as first_name,
            // person.father_name as father_name,
            // person.grand_name as grand_name,
            // person.family_name as family_name,
            person.national_no as national_no,
            person.phone_number as phone_number,
            person.principal_coordinator as principal_coordinator,
            person.sub_coordinator as sub_coordinator,
            person.primary_key as primary_key,
            COUNT(DISTINCT relative) AS num_relatives
        ORDER BY num_relatives DESC
        LIMIT 100;
    """
    # box search
    MATCH_BLOCK = build_match_block_from_location(filters)

    q = f"""
        {MATCH_BLOCK}
        WITH box
        // Traverse family relations up to n degrees and check if they vote at the same box
        MATCH (person)-[:{target_relationships}*1..{target_degrees}]->(relative:Person)-[:VOTES_AT]->(b)
        WITH person, relative
        {RETURN_BLOCK}
    """
    print('execuitng query ... ')
    data = run_query(q)

    return q, pd.DataFrame(data)

def get_person_influence(filters):
    target_national_no = filters.get('national_no')
    target_relationships = f"({'|'.join(filters.get('relationship'))})"
    target_degrees = filters.get('degree', '1')

    q = f"""
        MATCH (voter:Person {{national_no: '{target_national_no}'}}) -[:({target_relationships})*1..{target_degrees}]- (relatives)
        MATCH (relatives) -[r1:VOTES_AT]-> (b1)
        MATCH (voter) -[r2:VOTES_AT]-> (b2)
        WITH voter, relatives, collect(distinct b1) + collect(distinct b2) AS b, r1, r2
        UNWIND b AS singleb
        OPTIONAL MATCH (ce) -[r3:HAS_BOX]-> (singleb)
        UNWIND ce AS singlece
        MATCH (c) -[r4:HAS_CENTER]-> (singlece)
        RETURN voter, relatives, b, ce, c, r1, r2, r3, r4
    """

    with get_driver().session() as session: #type: ignore
        res = session.run(q)
    
    # build graph
    n4j_graph = res.graph()

    G = nx.DiGraph()
    for node in n4j_graph.nodes:
        G.add_node(node.id, **node.properties)
    
    for edge in n4j_graph.relationships():
        G.add_edge(edge.start_node.id, edge.end_node.id, **edge.properties)
    
    return G

def run_clef(filters):
    target_box = filters.get('box')
    target_center = filters.get('center')
    target_circle = filters.get('circle')
    raw_relationships = filters.get('relationship')
    target_relationships = f"({'|'.join(filters.get('relationship'))})"

    target_set_size = filters.get('seedSetSize', 10)
    target_monte_carlo = filters.get('monteCarloSimulations', 1000)
    target_probability = filters.get('probability', 0.1)
    target_degrees = filters.get('degree', '1')

    if target_box:
        MATCH_BLOCK = f"""
            MATCH (person)-[:{target_relationships}*1..{target_degrees}]-(relative:Person)-->(box:Box)
            WHERE elementId(box) = '{target_box}'
            
        """
    elif target_center:
        MATCH_BLOCK = f"""
            MATCH (person)-[:{target_relationships}*1..{target_degrees}]-(relative:Person)-->(box:Box)--(center:Center)
            WHERE elementId(center) = '{target_center}'
        """
    else:
        MATCH_BLOCK = f"""
            MATCH (person)-[:{target_relationships}*1..{target_degrees}]-(relative:Person)-->(box:Box)--(center:Center)--(circle:Circle)
            WHERE elementId(circle) = '{target_circle}'
        """
    
    
    gds = get_gds()
    
    # build graph projection
    _projection_name = str(uuid4())
    
    build_q = f"""
        {MATCH_BLOCK}
        RETURN gds.graph.project('{_projection_name}', person, relative)
    """
    
    G, res = gds.graph.cypher.project(build_q)
    print(res)
    print(f"Graph '{G.name()}' node count: {G.node_count()}")
    print(f"Graph '{G.name()}' node labels: {G.node_labels()}")
    print(f"Graph '{G.name()}' relationship count: {G.relationship_count()}")

    # run clef
    clef_result = gds.beta.influenceMaximization.celf.stream(
        G=G,
        seedSetSize=target_set_size,
        monteCarloSimulations=target_monte_carlo,
        propagationProbability=target_probability
    )
    
    # augment with node details
    _props = ['score', 'full_name', 'national_no', 'phone_number', 'principal_coordinator', 'sub_coordinator', 'primary_key']
    out = []
    for tup in clef_result.itertuples():
        out.append(gds.util.asNode(tup.nodeId)._properties | {'score': tup.spread})
    
    out = pd.DataFrame(out)[_props].sort_values('score', ascending=False)
    return out
# --------------------------------------------------------------------------------------------------
