from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
driver = GraphDatabase.driver(URI)  # No auth needed

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
        MATCH (t:Title)
        RETURN t.value AS title, t.uri AS uri
        LIMIT $limit
        """
        params = {"limit": limit}
        
        result = session.run(query, **params)
        return [{"s": {"value": record["uri"]}, "o": {"value": record["title"]}} for record in result]

# Return the IRI (direct URI)
def get_etd_link(iri):
    """Get link for an ETD by IRI in Neo4j"""
    try:
        with driver.session() as session:
            # Get URI from Title node
            result = session.run(
                """
                MATCH (t:Title)
                WHERE t.uri = $iri
                RETURN t.uri as link
                LIMIT 1
                """,
                iri=iri
            )
            
            record = result.single()
            if record and record["link"]:
                return record["link"]
            
            # If no URI property found, return the IRI itself
            return iri
    except Exception as e:
        print(f"Error in get_etd_link: {str(e)}")
        return iri

# Return metadata for a specific ETD node
def get_etd_metadata(iri):
    """Retrieve metadata for a specific ETD by its IRI with proper formatting"""
    try:
        with driver.session() as session:
            # Get the Title node info
            title_result = session.run(
                """
                MATCH (t:Title {uri: $iri})
                RETURN t.value as title, t.id as id, t.uri as uri
                """,
                iri=iri
            ).single()
            
            if not title_result:
                return []
            
            # Prepare metadata list
            metadata = []
            
            # Add title and ID
            metadata.append(f"hasTitle:{title_result['title']}")
            metadata.append(f"ID:{title_result['id']}")
            
            # Get Author
            author_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:HAS_AUTHOR]->(a:Author)
                RETURN a.name as author
                """,
                iri=iri
            ).single()
            if author_result and author_result['author']:
                metadata.append(f"Author:{author_result['author']}")
            
            # Get Advisor
            advisor_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:ACADEMIC_ADVISOR]->(a:Advisor)
                RETURN a.name as advisor
                """,
                iri=iri
            ).single()
            if advisor_result and advisor_result['advisor']:
                metadata.append(f"academicAdvisor:{advisor_result['advisor']}")
            
            # Get Year
            year_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:PUBLISHED_IN]->(y:Year)
                RETURN y.value as year
                """,
                iri=iri
            ).single()
            if year_result and year_result['year']:
                metadata.append(f"issuedDate:{year_result['year']}")
            
            # Get University
            university_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:PUBLISHED_BY]->(u:University)
                RETURN u.name as university
                """,
                iri=iri
            ).single()
            if university_result and university_result['university']:
                metadata.append(f"publishedBy:{university_result['university']}")
            
            # Get Department
            department_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:ACADEMIC_DEPARMENT]->(d:Department)
                RETURN d.name as department
                """,
                iri=iri
            ).single()
            if department_result and department_result['department']:
                metadata.append(f"academicDepartment:{department_result['department']}")

            # Get Degree
            degree_query = """
            MATCH (t:Title {uri: $iri})-[:DEGREE_TYPE]->(d:Degree)
            RETURN d.name as degree
            """
            degree_result = session.run(degree_query, iri=iri).single()
            if degree_result and degree_result['degree']:
                metadata.append(f"degree:{degree_result['degree']}")

            # Get Discipline
            discipline_query = """
            MATCH (t:Title {uri: $iri})-[:ACADEMIC_DISCIPLINE]->(d:Discipline)
            RETURN d.name as discipline
            """
            discipline_result = session.run(discipline_query, iri=iri).single()
            if discipline_result and discipline_result['discipline']:
                metadata.append(f"discipline:{discipline_result['discipline']}")
                        
            # Get Abstract
            abstract_result = session.run(
                """
                MATCH (t:Title {uri: $iri})-[:HAS_ABSTRACT]->(a:Abstract)
                RETURN a.text as abstract
                """,
                iri=iri
            ).single()
            if abstract_result and abstract_result['abstract']:
                metadata.append(f"hasAbstract:{abstract_result['abstract']}")
            
            # Add URI
            metadata.append(f"URI:{iri}")
            
            # Add empty degree and discipline
            metadata.append(f"degree:")
            metadata.append(f"discipline:")
            
            return metadata
            
    except Exception as e:
        print(f"Error in get_etd_metadata: {str(e)}")
        return [f"Error:{str(e)}"]

def search_etds_by_keyword(keyword, limit=100, pred="title"):
    """Search ETDs by keyword in the specified metadata field"""
    with driver.session() as session:
        # Handle different field types by targeting the right node type
        if pred == "title":
            query = """
            MATCH (t:Title)
            WHERE toLower(t.value) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "author":
            query = """
            MATCH (t:Title)-[:HAS_AUTHOR]->(a:Author)
            WHERE toLower(a.name) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "advisor":
            query = """
            MATCH (t:Title)-[:ACADEMIC_ADVISOR]->(a:Advisor)
            WHERE toLower(a.name) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "abstract":
            query = """
            MATCH (t:Title)-[:HAS_ABSTRACT]->(a:Abstract)
            WHERE toLower(a.text) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "institution":
            query = """
            MATCH (t:Title)-[:PUBLISHED_BY]->(u:University)
            WHERE toLower(u.name) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "department":
            query = """
            MATCH (t:Title)-[:ACADEMIC_DEPARTMENT]->(d:Department)
            WHERE toLower(d.name) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        elif pred == "year":
            query = """
            MATCH (t:Title)-[:PUBLISHED_IN]->(y:Year)
            WHERE toLower(y.value) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        else:
            # Default case - search in title
            query = """
            MATCH (t:Title)
            WHERE toLower(t.value) CONTAINS toLower($kw)
            RETURN t.uri AS s, t.value AS title
            LIMIT $limit
            """
        
        result = session.run(query, kw=keyword, limit=limit)
        return [{"s": {"value": record["s"]}, "title": {"value": record["title"]}} for record in result]

def get_etds_by_year(year, limit=100):
    """Get ETDs from a specific year"""
    with driver.session() as session:
        try:
            year_str = str(year)  # Convert to string for comparison with the Year node value
        except ValueError:
            print("Invalid year format.")
            return []

        result = session.run(
            """
            MATCH (t:Title)-[:PUBLISHED_IN]->(y:Year)
            WHERE y.value = $year
            RETURN t.value AS title, t.uri AS uri
            LIMIT $limit
            """,
            year=year_str,
            limit=limit
        )
        rows = [{"s": {"value": record["uri"]}, "title": {"value": record["title"]}} for record in result]
        return rows

def get_etd_count():
    """Get total count of ETDs in Neo4j database"""
    with driver.session() as session:
        result = session.run("MATCH (t:Title) RETURN count(t) AS count")
        count = result.single()["count"]
        return count
