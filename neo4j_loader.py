from neo4j import GraphDatabase
import json

# Connect to Neo4j 
driver = GraphDatabase.driver("bolt://localhost:7687")

def load_etds_to_neo4j(json_path):
    with open(json_path, 'r') as f:
        etds = json.load(f)

    with driver.session() as session:
        for etd in etds:
            uri = etd.get("uri") or f"http://localhost:7474/etd/{etd['id']}"
            title = etd.get("title", "")
            author = etd.get("author", "")
            year = etd.get("year", "")
            advisor = etd.get("advisor", "")
            university = etd.get("university", "")
            department = etd.get("department", "")
            abstract = etd.get("abstract", "")
            keywords = etd.get("keywords", [])

            # Create ETD node
            session.run("""
                MERGE (e:ETD {uri: $uri})
                SET e.title = $title,
                    e.author = $author,
                    e.year = $year,
                    e.advisor = $advisor,
                    e.university = $university,
                    e.department = $department,
                    e.abstract = $abstract
            """, uri=uri, title=title, author=author, year=year,
                 advisor=advisor, university=university,
                 department=department, abstract=abstract)

            # Add keyword nodes and relationships
            for keyword in keywords:
                if keyword:
                    session.run("""
                        MERGE (k:Keyword {name: $keyword})
                        MATCH (e:ETD {uri: $uri})
                        MERGE (e)-[:HAS_KEYWORD]->(k)
                    """, keyword=keyword, uri=uri)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load ETD metadata into Neo4j")
    parser.add_argument("json_file", help="Path to the JSON file containing ETD metadata")
    args = parser.parse_args()

    load_etds_to_neo4j(args.json_file)
    print("Completed loading ETDs into Neo4j")
