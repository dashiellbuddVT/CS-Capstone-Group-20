from neo4j import GraphDatabase

AURA_URI = "neo4j+s://3adec16a.databases.neo4j.io"
USERNAME = "neo4j"
PASSWORD = "jzalO94sCd2UeveJIewPIPlWH1JIJ9vGZ0e3i9URgNc"

driver = None

def connect_to_neo4j():
    global driver
    if driver is None:
        try:
            driver = GraphDatabase.driver(AURA_URI, auth=(USERNAME, PASSWORD))
            driver.verify_connectivity()
            print(" Connected to Neo4j Aura successfully!")
        except Exception as e:
            print("Aura connection failed:", e)
            driver = None
    return driver


def get_etd_count():
    connect_to_neo4j()
    if driver is None:
        return 0
    with driver.session() as session:
        result = session.run("MATCH (e:ETD) RETURN count(e) AS count")
        record = result.single()
        return record["count"] if record else 0


def get_etd_titles(limit=100):
    connect_to_neo4j()
    if driver is None:
        return []
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD)
            RETURN e.uri AS uri, e.title AS title
            LIMIT $limit
            """,
            limit=limit
        )
        return [{"s": {"value": r["uri"]}, "o": {"value": r["title"]}} for r in result]


def search_etds_by_keyword(keyword, limit=100):
    connect_to_neo4j()
    if driver is None:
        return []
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD)
            WHERE toLower(e.title) CONTAINS toLower($kw)
            RETURN e.uri AS uri, e.title AS title
            LIMIT $limit
            """,
            kw=keyword,
            limit=limit
        )
        return [{"s": {"value": r["uri"]}, "title": {"value": r["title"]}} for r in result]


def get_etds_by_year(year, limit=100):
    connect_to_neo4j()
    if driver is None:
        return []
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD)
            WHERE e.year = $year
            RETURN e.uri AS uri, e.title AS title
            LIMIT $limit
            """,
            year=year,
            limit=limit
        )
        return [{"s": {"value": r["uri"]}, "title": {"value": r["title"]}} for r in result]


def get_etd_metadata(iri):
    connect_to_neo4j()
    if driver is None:
        return ["Neo4j not connected"]
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD {uri: $iri})
            RETURN e.title AS hasTitle,
                   e.author AS Author,
                   e.year AS issuedDate,
                   e.university AS publishedBy,
                   e.abstract AS hasAbstract
            """,
            iri=iri
        )
        record = result.single()
        if not record:
            return ["No metadata found"]
        return [f"{k}: {v}" for k, v in record.items() if v]


def get_etd_link(iri):
    return iri
