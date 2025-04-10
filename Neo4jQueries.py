from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
driver = GraphDatabase.driver("bolt://localhost:7687")  # No auth needed

# Connect to the local Neo4j database
driver = GraphDatabase.driver(URI)

# Verify connection
try:
    driver.verify_connectivity()
    print("Connected to local Neo4j successfully!")
except Exception as e:
    print("Connection failed:", e)


# Get list (title search)
def get_etd_titles(keyword):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD)
            WHERE toLower(e.title) CONTAINS toLower($kw)
            RETURN e.title AS title, e.uri AS uri
            LIMIT 100
            """, kw=keyword
        )
        return [{"s": {"value": record["uri"]}, "o": {"value": record["title"]}} for record in result]


# Return the IRI (direct URI)
def get_etd_link(iri):
    return iri  # We store and return the URI directly


# Return metadata for a specific ETD node
def get_etd_metadata(iri):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD {uri: $iri})
            RETURN e.title AS title, e.author AS author, e.year AS year, e.advisor AS advisor
            """,
            iri=iri
        )
        record = result.single()
        return [f"{k}: {v}" for k, v in record.items()] if record else ["No metadata found"]

def search_etds_by_keyword(keyword, limit=100):
    """Search ETDs by keyword in the title"""
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD)
            WHERE toLower(e.title) CONTAINS toLower($kw)
            RETURN e.uri AS s, e.title AS title
            LIMIT $limit
            """,
            kw=keyword,
            limit=limit
        )
        return [{"s": {"value": record["s"]}, "title": {"value": record["title"]}} for record in result]

def get_etds_by_year(year, limit=100):
    with driver.session() as session:
        try:
            year = int(year)  # Ensure numeric comparison
        except ValueError:
            print("Invalid year format. Must be an integer.")
            return []

        result = session.run(
            """
            MATCH (e:ETD)
            WHERE e.year = $year
            RETURN e.title AS title, e.uri AS uri
            LIMIT $limit
            """,
            year=year,
            limit=limit
        )
        rows = [{"s": {"value": record["uri"]}, "title": {"value": record["title"]}} for record in result]
        return rows
