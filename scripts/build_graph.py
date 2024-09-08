import sys
import polars as pl
from neo4j import GraphDatabase
import subprocess
import time

def restart_neo4j():
    print("Restarting Neo4j ...")
    # command = ['sudo', 'neo4j', 'restart']
    command = ['sudo', 'systemctl', 'restart', 'neo4j.service']
    subprocess.run(command, check=True)
    time.sleep(30)

def init_constraints(n4j__uri: str, n4j__user: str, n4j__password: str):
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))

    # # Define the Cypher queries for constraints
    with driver.session() as session: # type: ignore
        result = session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:Box) REQUIRE (b.name, b.center, b.circle) IS UNIQUE")


def update_campaign_data(
    src_file: str,
    n4j__uri: str,
    n4j__user: str,
    n4j__password: str,
):
    print(f"Updating campaign data from {src_file} ...")
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))

    dest_file = f'/var/lib/neo4j/import/staged.csv'
    
    command = ['sudo', 'cp', src_file, dest_file]
    subprocess.run(command, check=True)

    with driver.session() as session: # type: ignore
        result = session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///staged.csv' AS row
            MATCH (p:Person {primary_key: row.primary_key})
            SET
                p.phone_number = toString(row.phone_number),
                p.credibility = row.credibility,
                p.type = row.type,
                p.principal_coordinator = row.principal_coordinator,
                p.sub_coordinator = row.sub_coordinator,
                p.Y_2013 = row.Y_2013,
                p.Y_2016 = row.Y_2016,
                p.Y_2020 = row.Y_2020,
                p.Y_2021 = row.Y_2021,
                p.Y_2024 = row.Y_2024
        """)
        
    command = ['sudo', 'rm', dest_file]
    subprocess.run(command, check=True)

def load_from_raw(
  raw_df_path: str,
  n4j__uri: str,
  n4j__user: str,
  n4j__password: str,
  csv_chunk_size: int = 100_000      
):
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    CHUNK_SIZE = csv_chunk_size

    # load data
    # ----------------------------------------------------------------------------------------------
    print(f"Loading data from {raw_df_path} ...")
    df = (
        pl.read_csv(raw_df_path)
        .filter(pl.col('is_unique_shrink_name') & pl.col('unmatched') == 0)
        .filter((pl.col('national_no') != 'missing'))
        .select([
            'full_name',
            'first_name',
            'father_name',
            'grand_name',
            'family_name',
            'national_no',
            'father_national_no',
            'mother_national_no',
            'new_big_key',
            'dob',
            'age',
            'religion',
            'address',
            'circle',
            'center',
            'box',
            'primary_key',
        ])
        # drop duplicate national_no keep first
        .unique('national_no', keep='first')
    )
    print(f"Data loaded: {len(df)} records")

    print(f"Checking for missing fathers and mothers ...")

    father_nos = df['father_national_no'].drop_nulls().unique().to_list()
    mother_nos = df['mother_national_no'].drop_nulls().unique().to_list()
    nos = df['national_no'].drop_nulls().unique().to_list()

    missing_fathers = list(set(father_nos) - set(nos))
    missing_mothers = list(set(mother_nos) - set(nos))

    # create dataframe with national_no for missing fathers and mothers
    missing_fathers_df = (
        pl.DataFrame({ 'national_no': missing_fathers})
        .filter(pl.col('national_no') != 'missing')
        .filter(pl.col('national_no').cast(pl.Utf8).str.len_chars() == 10)
    )
    missing_mothers_df = (
        pl.DataFrame({'national_no': missing_mothers})
        .filter(pl.col('national_no') != 'missing')
        .filter(pl.col('national_no').cast(pl.Utf8).str.len_chars() == 10)
    )
    print(f"Missing fathers: {len(missing_fathers_df)}")
    print(f"Missing mothers: {len(missing_mothers_df)}")


    df_cirlce_center_box = df.select(['center','box', 'circle']).unique()
    # ----------------------------------------------------------------------------------------------

    total_nodes = (
        len(df) + len(missing_fathers_df) + len(missing_mothers_df)
        + df_cirlce_center_box.select(['circle']).n_unique()
        + df_cirlce_center_box.select(['center', 'circle']).n_unique()
        + df_cirlce_center_box.select(['box', 'center', 'circle']).n_unique()
    )

    print(f"=*"*50)
    print(f"=*"*50)
    print(f"Total nodes to build: {total_nodes}")
    print(f"=*"*50)
    print(f"=*"*50)

    # build circle / center / box nodes and relationships
    # ----------------------------------------------------------------------------------------------
    print(f"Building circle / center / box nodes and relationships ...")
    with driver.session() as session: # type: ignore
        for box_dict in df_cirlce_center_box.to_dicts():
            session.run(
            """
            MERGE (c:Circle {name:$circle})
            MERGE (ce:Center {name:$center, circle: $circle})
            MERGE (b:Box {name: toString($box), center: $center, circle: $circle})
            MERGE (c)-[:HAS_CENTER]->(ce)
            MERGE (ce)-[:HAS_BOX]->(b)
            """,
            circle=box_dict['circle'],
            center=box_dict['center'],
            box=box_dict['box']
        )
    print(f"Circle / Center / Box nodes and relationships built ...")
    # ----------------------------------------------------------------------------------------------

    # build person nodes
    # ----------------------------------------------------------------------------------------------
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))

    print(f"Building person nodes ...")
    for start in range(0, len(df), CHUNK_SIZE):
        stop = start + CHUNK_SIZE if start + CHUNK_SIZE < len(df) else len(df)
        print(f"> Processing {start}:{stop} ...")

        src_file = f'staged.csv'
        dest_file = f'/var/lib/neo4j/import/staged.csv'
        
        df[start:stop].write_csv(src_file)
        command = ['sudo', 'mv', src_file, dest_file]
        subprocess.run(command, check=True)
        
        with driver.session() as session: # type: ignore
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///staged.csv' AS row
                CREATE (p:Person {
                    full_name: row.full_name,
                    first_name: row.first_name,
                    father_name: row.father_name,
                    grand_name: row.grand_name,
                    family_name: row.family_name,
                    national_no: row.national_no,
                    father_national_no: row.father_national_no,
                    mother_national_no: row.mother_national_no,
                    new_big_key: row.new_big_key,
                    dob: row.dob,
                    age: row.age,
                    religion: reow.religion,
                    address: row.address,
                    circle: row.circle,
                    center: row.center,
                    box: toString(row.box),
                    primary_key: row.primary_key,
                    is_missing: false,
                                 
                    phone_number: 'missing',
                    credibility: 'missing',
                    type: 'missing',
                    principal_coordinator: 'missing',
                    sub_coordinator: 'missing',
                    Y_2013: 'missing',
                    Y_2016: 'missing',
                    Y_2020: 'missing',
                    Y_2021: 'missing',
                    Y_2024: 'missing'
                })
            """)
        
        command = ['sudo', 'rm', dest_file]
        subprocess.run(command, check=True)
    print(f"Person nodes built ...")
    # ----------------------------------------------------------------------------------------------

    # handle non-matching fathers and mothers
    # ----------------------------------------------------------------------------------------------
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    
    print(f"Handling non-matching fathers and mothers ...")
    for target_df in [missing_fathers_df, missing_mothers_df]:
        for start in range(0, len(target_df), CHUNK_SIZE):
            stop = start + CHUNK_SIZE if start + CHUNK_SIZE < len(target_df) else len(target_df)
            print(f"> Processing {start}:{stop} ...")

            src_file = f'staged.csv'
            dest_file = f'/var/lib/neo4j/import/staged.csv'
            
            target_df[start:stop].write_csv(src_file)
            command = ['sudo', 'mv', src_file, dest_file]
            subprocess.run(command, check=True)
            
            with driver.session() as session: # type: ignore
                result = session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///staged.csv' AS row
                    CREATE (p:Person {
                        full_name: 'missing',
                        first_name: 'missing',
                        father_name: 'missing',
                        grand_name: 'missing',
                        family_name: 'missing',
                        national_no: row.national_no,
                        father_national_no: 'missing',
                        mother_national_no: 'missing',
                        new_big_key: 'missing',
                        dob: 'missing',
                        age: 'missing',
                        religion: 'missing',
                        address: 'missing',
                        circle: 'missing',
                        center: 'missing',
                        box: 'missing',
                        is_missing: true,
                                     
                        phone_number: 'missing',
                        credibility: 'missing',
                        type: 'missing',
                        principal_coordinator: 'missing',
                        sub_coordinator: 'missing',
                        Y_2013: 'missing',
                        Y_2016: 'missing',
                        Y_2020: 'missing',
                        Y_2021: 'missing',
                        Y_2024: 'missing'
                    })
                """)
            
            command = ['sudo', 'rm', dest_file]
            subprocess.run(command, check=True)
    print(f"Non-matching fathers and mothers handled ...")
    # ----------------------------------------------------------------------------------------------

    # build indexes
    # ----------------------------------------------------------------------------------------------
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))

    with driver.session() as session: # type: ignore
        result = session.run("CREATE INDEX box__name_index IF NOT EXISTS FOR (b:Box) ON (b.name)")
        result = session.run("CREATE INDEX box__center_index IF NOT EXISTS FOR (b:Box) ON (b.center)")
        result = session.run("CREATE INDEX box__circle_index IF NOT EXISTS FOR (b:Box) ON (b.circle)")

        result = session.run("CREATE INDEX person__national_no_index IF NOT EXISTS FOR (n:Person) ON (n.national_no)")
        result = session.run("CREATE INDEX person__mother_national_no_index IF NOT EXISTS FOR (n:Person) ON (n.mother_national_no)")
        result = session.run("CREATE INDEX person__father_national_no_index IF NOT EXISTS FOR (n:Person) ON (n.father_national_no)")
        result = session.run("CREATE INDEX person__pk IF NOT EXISTS FOR (n:Person) ON (n.primary_key)")
        result = session.run("CREATE INDEX person__is_missing_index IF NOT EXISTS FOR (n:Person) ON (n.is_missing)")
    # ----------------------------------------------------------------------------------------------

def create_relationships(n4j__uri: str, n4j__user: str, n4j__password: str):
    # create relationships
    # ----------------------------------------------------------------------------------------------

    # create father relationships
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))

    print(f"Creating father relationships ...")
    with driver.session() as session: # type: ignore
        result = session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (child:Person) WHERE child.father_national_no <> 'missing' RETURN child",
                "OPTIONAL MATCH (father:Person {national_no: child.father_national_no})  WHERE father IS NOT NULL  WITH father, child WHERE father IS NOT NULL CREATE (father)-[:FATHER]->(child)",                 
                {batchSize:50000, parallel:false}
            ) YIELD batches, total, errorMessages RETURN batches, total, errorMessages;""" 
        )

        _res = result.single()
        if _res is not None:
                for k, v in _res.items():
                    print(f"{k}: {v}")
        
    # create mother relationships
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    
    print(f"Creating mother relationships ...")
    with driver.session() as session: # type: ignore
        result = session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (child:Person) WHERE child.mother_national_no <> 'missing' RETURN child",
                "OPTIONAL MATCH (mother:Person {national_no: child.mother_national_no})  WHERE mother IS NOT NULL  WITH mother, child WHERE mother IS NOT NULL CREATE (mother)-[:MOTHER]->(child)",                 
                {batchSize:50000, parallel:false}                 
            )
            """ 
        )

        _res = result.single()
        if _res is not None:
                for k, v in _res.items():
                    print(f"{k}: {v}")

    # create spouse relationships
    restart_neo4j()
    
    print(f"Creating spouse relationships ...")
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    with driver.session() as session: # type: ignore
        result = session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (father:Person)-[:FATHER]->(child:Person)<-[:MOTHER]-(mother:Person) WHERE NOT (father)-[:SPOUSE]-(mother) RETURN father, mother",
                "MATCH (father), (mother) MERGE (father)-[:SPOUSE]->(mother)",
                {batchSize: 50000, parallel: false}
            )
            """
        )

        _res = result.single()
        if _res is not None:
                for k, v in _res.items():
                    print(f"{k}: {v}")

    # create sibling relationships
    restart_neo4j()
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    print(f"Creating sibling relationships ...")
    with driver.session() as session: # type: ignore
        result = session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (p1:Person)<-[:FATHER]-(father:Person)-[:FATHER]->(p2:Person) WHERE id(p1) > id(p2) RETURN DISTINCT p1, p2",
                "MATCH (p1), (p2) MERGE (p1)-[:SIBLING]->(p2)",
                {batchSize: 50000, parallel: false}
            )
            """
        )

        _res = result.single()
        if _res is not None:
                for k, v in _res.items():
                    print(f"{k}: {v}")
    
    # create votes at box level
    restart_neo4j()
    print(f"Creating vote at box relationships ...")
    driver = GraphDatabase.driver(n4j__uri, auth=(n4j__user, n4j__password))
    with driver.session() as session: # type: ignore
        result = session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (p:Person) WHERE p.box <> 'missing' RETURN p",
                "OPTIONAL MATCH (b:Box {name: p.box, center: p.center, circle: p.circle})  WHERE b IS NOT NULL  WITH b, p WHERE b IS NOT NULL MERGE (p)-[:VOTES_AT]->(b)",                 
                {batchSize:50000, parallel:false}
            )
            """ 
        )

        _res = result.single()
        if _res is not None:
                for k, v in _res.items():
                    print(f"{k}: {v}")
    # ----------------------------------------------------------------------------------------------
    restart_neo4j()
    
if __name__ == '__main__':
    raw_df_path = sys.argv[1]
    campaign_df_path = sys.argv[2]
    n4j__uri = sys.argv[3]
    n4j__user = sys.argv[4]
    n4j__password = sys.argv[5]

    print("Running build_graph.py with the following arguments:")
    print(f"> raw_df_path: {raw_df_path}")
    print(f"> campaign_df_path: {campaign_df_path}")
    print(f"> n4j__uri: {n4j__uri}")
    print(f"> n4j__user: {n4j__user}")
    print(f"> n4j__password: {n4j__password}")
    print('-'*50)

    init_constraints(n4j__uri, n4j__user, n4j__password)

    # load_from_raw(
    #     raw_df_path,
    #     n4j__uri,
    #     n4j__user,
    #     n4j__password,
    #     csv_chunk_size=100_000
    # )

    create_relationships(
        n4j__uri,
        n4j__user,
        n4j__password,
    )

    update_campaign_data(
        campaign_df_path,
        n4j__uri,
        n4j__user,
        n4j__password,
    )