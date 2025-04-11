from neo4j import GraphDatabase
import requests
from requests.auth import HTTPBasicAuth

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
def get_etd_titles(limit=100):
    """Retrieve ETD titles and URIs with a limit"""
    with driver.session() as session:
        query = """
        MATCH (e:ETD)
        RETURN e.title AS title, e.uri AS uri
        LIMIT $limit
        """
        params = {"limit": limit}
        
        result = session.run(query, **params)
        return [{"s": {"value": record["uri"]}, "o": {"value": record["title"]}} for record in result]


# Return the IRI (direct URI)
# def get_etd_link(iri):
#     return iri  # We store and return the URI directly
def get_etd_link(iri):
    """
    Get link (URI) for an ETD by IRI from Neo4j.

    Args:
        iri (str): The IRI of the ETD node.

    Returns:
        str or None: The URI (link) if found, else None.
    """
    cypher_query = {
        "statements": [
            {
                "statement": "MATCH (e:ETD {iri: $iri}) RETURN e.uri AS link LIMIT 1",
                "parameters": {"iri": iri}
            }
        ]
    }

    response = requests.post(
        URI,
        json=cypher_query,
        #auth=HTTPBasicAuth(USERNAME, PASSWORD)
    )

    try:
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results and results[0]["data"]:
                return results[0]["data"][0]["row"][0]
    except Exception as e:
        print(f"Error parsing Neo4j response: {e}")

    return None


# Return metadata for a specific ETD node
# def get_etd_metadata(iri):
#     with driver.session() as session:
#         result = session.run(
#             """
#             MATCH (e:ETD {uri: $iri})
#             RETURN e.title AS Title, e.author AS Author, e.year AS Year, e.advisor AS Advisor, e.abstract AS Abstract
#             """,
#             iri=iri
#         )
#         record = result.single()
#         return [f"{k}: {v}" for k, v in record.items()] if record else ["No metadata found"]
import textwrap
def get_etd_metadata(iri, line_length=80):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:ETD {uri: $iri})
            RETURN e.title AS Title, e.author AS Author, e.year AS Year, e.advisor AS Advisor, e.abstract AS Abstract
            """,
            iri=iri
        )
        record = result.single()
        if record:
            metadata = []
            for k, v in record.items():
                if k == "Abstract":
                    v = '\n'.join(textwrap.wrap(v, width=line_length))
                metadata.append(f"{k}: {v}")
            return metadata
        else:
            return ["No metadata found"]



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


def get_etd_count():
    """Get total count of ETDs in Neo4j database"""
    with driver.session() as session:
        result = session.run("MATCH (e:ETD) RETURN count(e) AS count")
        count = result.single()["count"]
        return count

