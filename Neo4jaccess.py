from neo4j import GraphDatabase

# For local Neo4j desktop/server with no auth:
URI       = "bolt://localhost:7687"
AUTH      = None

# If you want to use Aura, uncomment and fill in:
# URI       = "neo4j+s://3adec16a.databases.neo4j.io"
# AUTH      = ("neo4j", "jzal094sCd2UeveJIewPIPlWH1JIJ9vGZ0e3i9URgNc")

driver = None

def connect_to_neo4j():
    """Lazily initialize and verify connectivity once."""
    global driver
    if driver is None:
        if AUTH:
            driver = GraphDatabase.driver(URI, auth=AUTH)
        else:
            driver = GraphDatabase.driver(URI)
        try:
            driver.verify_connectivity()
        except Exception as e:
            print(" Unable to connect to Neo4j:", e)
            driver = None
    return driver

def get_etd_count():
    """Return the total number of :ETD nodes."""
    if not connect_to_neo4j():
        return 0
    with driver.session() as session:
        rec = session.run("MATCH (e:ETD) RETURN count(e) AS count").single()
        return rec["count"] if rec else 0

def search_etds_by_keyword(keyword, pred="title", limit=100):
    """
    Search ETDs by any property (title, author, advisor, abstract, etc.).
    Returns a list of {"s":{"value":uri}, "title":{"value":value}} dicts.
    """
    if not connect_to_neo4j():
        return []
    # only allow these fields for safety
    allowed = {"title","author","advisor","abstract","university","department","year"}
    if pred not in allowed:
        pred = "title"
    cypher = f"""
    MATCH (e:ETD)
    WHERE toLower(e.{pred}) CONTAINS toLower($kw)
    RETURN e.uri AS uri, e.{pred} AS value
    LIMIT $limit
    """
    with driver.session() as session:
        res = session.run(cypher, kw=keyword, limit=limit)
        return [
            {"s": {"value": r["uri"]}, "title": {"value": r["value"]}}
            for r in res
        ]

def get_etd_metadata(iri):
    """
    Retrieve all the standard metadata fields for one ETD node.
    Returns a list of strings like "hasTitle: The Title…", etc.
    """
    if not connect_to_neo4j():
        return []
    cypher = """
    MATCH (e:ETD {uri: $iri})
    RETURN
      e.title            AS hasTitle,
      e.author           AS Author,
      e.advisor          AS academicAdvisor,
      e.year             AS issuedDate,
      e.department       AS academicDepartment,
      e.university       AS publishedBy,
      e.abstract         AS hasAbstract
    """
    with driver.session() as session:
        rec = session.run(cypher, iri=iri).single()
        if not rec:
            return ["No metadata found"]
        out = []
        # keep the order your UI expects
        for key in [
            "hasTitle",
            "Author",
            "academicAdvisor",
            "issuedDate",
            "academicDepartment",
            "publishedBy",
            "hasAbstract"
        ]:
            v = rec.get(key)
            out.append(f"{key}: {v}" if v is not None else f"{key}: N/A")
        return out

def get_etd_link(iri):
    """The UI will simply open the stored URI in a new tab."""
    return iri
