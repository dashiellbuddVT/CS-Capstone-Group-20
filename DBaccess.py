from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON
import requests
from rdflib import Namespace
from requests.auth import HTTPDigestAuth
import json


endpoint_URL = "https://virtuoso.endeavour.cs.vt.edu/sparql-auth"
graph_URI = "http://localhost:8890/CSV#"
username = "dba"
password = "admin"

def sendQuery(q):
    headers = {
        "Content-Type": "application/sparql-update; charset=utf-8",
        "Accept": "application/sparql-results+json"
    }

    response = requests.post(
        endpoint_URL,
        data=q.encode('utf-8'),
        auth=HTTPDigestAuth(username, password),
        headers=headers
    )
    return response

def getnTitles(n):
    if n == "":
        n = 100
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{?s <http://localhost:8890/schemas/CSV/title> ?o}}
    LIMIT {n}
    """
    response = sendQuery(query)

    if response.status_code == 200:
        r = response.json()["results"]["bindings"]
        #print(json.dumps(r,indent=1))
        return r
    else:
        print(f"Failed: {response.status_code} {response.reason}")
        return


def getLink(IRI):
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{<{IRI}> <http://localhost:8890/schemas/CSV/uri> ?o}}
    LIMIT {200}
    """
    response = sendQuery(query)
    l = response.json()["results"]["bindings"][0]["o"]["value"]
    return l

def getMD(IRI):
    metaData = []
    query = f"""
    SELECT * FROM <{graph_URI}>
    WHERE {{<{IRI}> ?p ?o}}
    LIMIT {50}
    """
    response = sendQuery(query)
    bindings = response.json()["results"]["bindings"]
    print(bindings)
    
    for attribute in bindings:
        line = attribute["p"]["value"].replace("http://localhost:8890/schemas/CSV/" , "") + ": " + attribute["o"]["value"]
        metaData.append(line)
    return metaData[2:len(metaData) - 1]
