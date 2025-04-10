import requests
from requests.auth import HTTPDigestAuth
import json

# Configuration - same as in DBaccess.py
endpoint_URL = "https://virtuoso.endeavour.cs.vt.edu/sparql-auth"
graph_URI = "http://erdkb.endeavour.cs.vt.edu/ETDs"
username = "dba"
password = "admin"

def send_query(query):
    """Send a SPARQL query to the Virtuoso endpoint"""
    headers = {
        "Content-Type": "application/sparql-update; charset=utf-8",
        "Accept": "application/sparql-results+json"
    }

    response = requests.post(
        endpoint_URL,
        data=query.encode('utf-8'),
        auth=HTTPDigestAuth(username, password),
        headers=headers
    )
    return response

def get_etd_titles(limit=100):
    """Get ETD titles with limit"""
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{?s <http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasTitle> ?o}}
    LIMIT {limit}
    """
    response = send_query(query)

    if response.status_code == 200:
        return response.json()["results"]["bindings"]
    else:
        print(f"Failed: {response.status_code} {response.reason}")
        return []

def get_etd_link(iri):
    """Get link for an ETD by IRI"""
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{<{iri}> <http://etdkb.endeavour.cs.vt.edu/v1/predicate/identifier> ?o}}
    LIMIT 1
    """
    response = send_query(query)
    
    if response.status_code == 200 and response.json()["results"]["bindings"]:
        link = response.json()["results"]["bindings"][0]["o"]["value"]
        return link
    
    # If no URI is found, return the IRI itself as the link
    # This should work since our IRIs now match the web server routes
    return iri

def get_etd_metadata(iri):
    """Get all metadata for an ETD by IRI"""
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{<{iri}> ?p ?o}}
    LIMIT 50
    """
    response = send_query(query)
    
    if response.status_code != 200:
        print(f"Failed: {response.status_code} {response.reason}")
        return []
        
    bindings = response.json()["results"]["bindings"]
    metadata = []
    
    for attribute in bindings:
        prop = attribute["p"]["value"].replace("http://etdkb.endeavour.cs.vt.edu/v1/predicate/", "")
        value = attribute["o"]["value"]
        metadata.append(f"{prop}: {value}")
    
    return metadata

def search_etds_by_keyword(keyword, limit=100):
    """Search ETDs by keyword in title"""
    query = f"""
    SELECT DISTINCT ?s ?title FROM <{graph_URI}>
    WHERE {{
        ?s <http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasTitle> ?title .
        FILTER(CONTAINS(LCASE(?title), LCASE("{keyword}")))
    }}
    LIMIT {limit}
    """
    response = send_query(query)
    
    if response.status_code == 200:
        return response.json()["results"]["bindings"]
    else:
        print(f"Failed: {response.status_code} {response.reason}")
        return []

def get_etds_by_year(year, limit=100):
    """Get ETDs by publication year"""
    query = f"""
    SELECT ?s ?title FROM <{graph_URI}>
    WHERE {{
        ?s <http://etdkb.endeavour.cs.vt.edu/v1/predicate/issuedDate> "{year}" .
        ?s <http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasTitle> ?title .
    }}
    LIMIT {limit}
    """
    response = send_query(query)
    
    if response.status_code == 200:
        return response.json()["results"]["bindings"]
    else:
        print(f"Failed: {response.status_code} {response.reason}")
        return []

def get_etd_count():
    """Get total count of ETDs in database"""
    query = f"""
    SELECT (COUNT(DISTINCT ?s) as ?count) FROM <{graph_URI}>
    WHERE {{
        ?s <http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasTitle> ?title .
    }}
    """
    response = send_query(query)
    
    if response.status_code == 200 and response.json()["results"]["bindings"]:
        count = response.json()["results"]["bindings"][0]["count"]["value"]
        return int(count)
    else:
        print(f"Failed: {response.status_code} {response.reason}")
        return 0

# Test function
def test_queries():
    """Test the query functions"""
    print("Testing ETD queries...")
    
    # Get count
    count = get_etd_count()
    print(f"Total ETDs: {count}")
    
    # Get titles
    titles = get_etd_titles(5)
    print(f"\nFound {len(titles)} ETD titles:")
    for title in titles:
        print(f"- {title['o']['value']}")
    
    # Test metadata if we have titles
    if titles:
        first_iri = titles[0]["s"]["value"]
        print(f"\nMetadata for first ETD ({first_iri}):")
        metadata = get_etd_metadata(first_iri)
        for item in metadata:
            print(f"- {item}")
    
    print("\nTests completed")

if __name__ == "__main__":
    test_queries() 