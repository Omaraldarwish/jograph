import os

from dotenv import load_dotenv
import neo4j
import streamlit as st

import pandas as pd

DB_DIR = os.path.join(os.path.dirname(__file__), 'db')

# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

def get_driver():
    load_dotenv()
    uri = os.getenv('NEO4J__URI')
    user = os.getenv('NEO4J__USER')
    password = os.getenv('NEO4J__PASSWORD')

    return neo4j.GraphDatabase.driver(uri, auth=(user, password))

def run_query(query, **kwargs):
    with get_driver().session() as session: #type: ignore
        return session.run(query, **kwargs).data() 


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

# --------------------------------------------------------------------------------------------------

def get_circle_center_box_voter_counts(filters):
    
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
    


def get_family_counts(filters):
    
    target_box = filters.get('box')
    target_center = filters.get('center')
    target_circle = filters.get('circle')

    target_relationships = f"({'|'.join(filters.get('relationship'))})"
    target_degrees = filters.get('degree', '1')
    RETURN_BLOCK = """
        RETURN
            person.first_name,
            person.father_name,
            person.grand_name,
            person.family_name,
            person.national_no,
            person.phone_number,
            person.primary_key,
            COUNT(DISTINCT relative) AS num_relatives
        ORDER BY num_relatives DESC
        LIMIT 10;
    """
    BOX_QUERY = """

    """
    # box search
    if filters.get('box') is not None:
        MATCH_BLOCK = f"""
            MATCH (person:Person)-[:VOTES_AT]->(box:Box)
            WHERE elementId(box) = '{target_box}'
        """
    elif filters.get('center') is not None:
        MATCH_BLOCK = f"""
            MATCH (person:Person)-[:VOTES_AT]->(box:Box)--(center:Center)
            WHERE elementId(center) = '{target_center}'
        """
    else:
        MATCH_BLOCK = f"""
            MATCH (person:Person)-[:VOTES_AT]->(box:Box)--(center:Center)--(circle:Circle)
            WHERE elementId(circle) = '{target_circle}'
        """

    q = f"""
        {MATCH_BLOCK}
        WITH person, box
        // Traverse family relations up to n degrees and check if they vote at the same box
        UNWIND box as b
        MATCH (person)-[:{target_relationships}*1..{target_degrees}]->(relative:Person)-[:VOTES_AT]->(b)
        WITH person, relative
        {RETURN_BLOCK}
    """
    print('execuitng query ... ')
    data = run_query(q)

    return pd.DataFrame(data)
# --------------------------------------------------------------------------------------------------


# Base = declarative_base()

# class Circle(Base):
#     __tablename__ = 'circles'
    
#     circle_id = Column(String, primary_key=True)
#     circle_name = Column(String, unique=True)

#     centers = relationship('Center', back_populates='circle')

# class Center(Base):
#     __tablename__ = 'centers'

#     center_id = Column(String, primary_key=True)
#     center_name = Column(String)
#     circle_id = Column(String, ForeignKey('circles.circle_id'))

#     circle = relationship('Circle', back_populates='centers')
#     boxes = relationship('Box', back_populates='center')

# class Box(Base):
#     __tablename__ = 'boxes'

#     box_id = Column(String, primary_key=True)
#     box_name = Column(String)
#     center_id = Column(String, ForeignKey('centers.center_id'))

#     center = relationship('Center', back_populates='boxes')

# class BoxesTable:
#     def __init__(self, db_name='boxes.db', db_path='path_to_db', force_init=False):
#         if not os.path.exists(db_path):
#             os.makedirs(db_path)

#         self.engine = create_engine(f'sqlite:///{os.path.join(db_path, db_name)}')
#         Base.metadata.bind = self.engine
#         self.Session = sessionmaker(bind=self.engine)

#         if force_init or not self.is_initialized():
#             self.delete_tables()
#             self.create_tables()
#             self.init_data()

#     def is_initialized(self):
#         """Check if tables exist and have consistent counts of rows."""
#         session = self.Session()
#         inspector = inspect(self.engine)

#         if not inspector.has_table('circles') or not inspector.has_table('centers') or not inspector.has_table('boxes'):
#             return False
        
#         circle_count = session.query(Circle).count()
#         center_count = session.query(Center).count()
#         box_count = session.query(Box).count()

#         neo4j_counts = self.fetch_init_counts()

#         # Compare the counts
#         if (circle_count != neo4j_counts[0]['circle_count'] or
#             center_count != neo4j_counts[0]['center_count'] or
#             box_count != neo4j_counts[0]['box_count']):
#             session.close()
#             return False

#         session.close()
#         return True

#     def delete_tables(self):
#         """Drop tables."""
#         Base.metadata.drop_all(self.engine)

#     def create_tables(self):
#         """Create tables."""
#         Base.metadata.create_all(self.engine)

#     def fetch_init_counts(self):
#         q = """
#         MATCH (b:Box)MATCH (c:Circle)--(ce:Center)--(b:Box)
#         return
#             count(distinct c) as circle_count,
#             count(distinct ce) as center_count,
#             count(distinct b) as box_count
#         """

#         return run_query(q)

#     def init_data(self):
#         """Insert initial data into the tables."""
#         data = self.fetch_init_data()

#         session = self.Session()

#         _circle_ids = set()
#         _center_ids = set()
#         for entry in data:
#             # Insert data into Circle, Center, and Box
#             circle = Circle(circle_id=entry['circle_id'], circle_name=entry['circle_name'])
#             if circle.circle_id not in _circle_ids:
#                 _circle_ids.add(circle.circle_id)
#                 session.add(circle)
            
#             center = Center(center_id=entry['center_id'], center_name=entry['center_name'], circle_id=entry['circle_id'])
#             if center.center_id not in _center_ids:
#                 _center_ids.add(center.center_id)
#                 session.add(center)
                
#             box = Box(box_id=entry['box_id'], box_name=entry['box_name'], center_id=entry['center_id'])
#             session.add(box)

#         session.commit()
#         session.close()

#     def get_unique_circles(self):
#         """Return unique circle names."""
#         session = self.Session()
#         circles = session.query(Circle.circle_name, Circle.circle_id).distinct().all()
#         session.close()
#         return circles

#     def get_unique_centers(self):
#         """Return unique center names."""
#         session = self.Session()
#         centers = session.query(Center.center_name, Center.center_id).distinct().all()
#         session.close()
#         return centers

#     def get_unique_boxes(self):
#         """Return unique box names."""
#         session = self.Session()
#         boxes = session.query(Box.box_name, Box.box_id).distinct().all()
#         session.close()
#         return boxes

#     def get_centers_for_circle(self, circle_name):
#         """Return center names for a given circle name."""
#         session = self.Session()
#         centers = session.query(Center.center_name, Center.center_id).join(Circle).filter(Circle.circle_name == circle_name).all()
#         session.close()
#         return centers

#     def get_boxes_for_circle_and_center(self, circle_name, center_name):
#         """Return box names for a given circle name and center name."""
#         session = self.Session()
#         boxes = (
#             session.query(Box.box_name, Box.box_id)
#             .join(Center)
#             .join(Circle)
#             .filter(Circle.circle_name == circle_name, Center.center_name == center_name)
#             .all()
#         )
#         session.close()
#         return boxes

#     def close(self):
#         """Close the database session."""
#         self.Session.close_all()
