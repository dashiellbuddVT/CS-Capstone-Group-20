from py2neo import Graph

def connect_to_neo4j():
    graph = Graph("bolt://localhost:7687")

def getNeo4jTitles(n=100):
    graph = connect_to_neo4j()
    query = f"""
    MATCH (e:ETD)
    RETURN e.uri AS uri, e.title AS title
    LIMIT {n}
    """
    result = graph.run(query)
    return [{"s": {"value": row["uri"]}, "o": {"value": row["title"]}} for row in result]

def getNeo4jLink(uri):
    graph = connect_to_neo4j()
    query = f"""
    MATCH (e:ETD {{uri: '{uri}'}})
    RETURN e.uri AS link
    """
    return graph.evaluate(query)

def getNeo4jMD(uri):
    graph = connect_to_neo4j()
    query = f"""
    MATCH (e:ETD {{uri: '{uri}'}})
    RETURN e.title AS title, e.author AS author, e.advisor AS advisor,
           e.abstract AS abstract, e.year AS year, e.department AS department
    """
    result = graph.evaluate(query)
    if result:
        metadata = [
            f"title: {result['title']}",
            f"author: {result['author']}",
            f"advisor: {result['advisor']}",
            f"abstract: {result['abstract']}",
            f"year: {result['year']}",
            f"department: {result['department']}"
        ]
        return metadata
    return []
