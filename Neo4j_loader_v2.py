from neo4j import GraphDatabase
import json
import time
import sys
import re

# Connect to Neo4j 
driver = GraphDatabase.driver("bolt://localhost:7687")

def check_neo4j_version():
    try:
        with driver.session() as session:
            version = session.run("CALL dbms.components() YIELD name, versions, edition UNWIND versions as version RETURN name, version, edition").single()
            print(f"Connected to {version['name']} version {version['version']} {version['edition']} edition")
            return True
    except Exception as e:
        print(f"Error checking Neo4j version: {e}")
        return False

def clear_database():
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared successfully")
    except Exception as e:
        print(f"Error clearing database: {e}")
        return False
    return True

def verify_load():
    try:
        with driver.session() as session:
            # Check Title nodes
            title_count = session.run("MATCH (t:Title) RETURN count(t) as count").single()["count"]
            
            # These checks may return 0 if no relationships exist yet
            advisor_rel = session.run("MATCH ()-[r:HAS_ADVISOR]->() RETURN count(r) as count").single()["count"]
            year_rel = session.run("MATCH ()-[r:PUBLISHED_IN]->() RETURN count(r) as count").single()["count"]
            
            print(f"\nVerification results:")
            print(f"- Title nodes: {title_count}")
            print(f"- HAS_ADVISOR relationships: {advisor_rel}")
            print(f"- PUBLISHED_IN relationships: {year_rel}")
            
            if title_count == 0:
                print("WARNING: No Title nodes were loaded!")
                return False
            return True
    except Exception as e:
        print(f"Error verifying data: {e}")
        return False

def clean_id_field(value):
    """Clean ID field by removing <id> tags and extracting correct ID"""
    if not value:
        return ""
    
    # If it's a plain integer string, just return it
    if isinstance(value, str) and value.isdigit():
        return value
    
    # Try to extract the ID value inside <id> tags
    if isinstance(value, str) and "<id>" in value:
        # We want to ignore the <id> value and use the actual id field instead
        return ""
        
    # Otherwise return as is
    return str(value)

def load_etds_from_json(json_path):
    # Test connection first
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            if not record or record.get("test") != 1:
                print("Error: Could not validate Neo4j connection")
                return False
            
        print("Connection to Neo4j successful")
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        return False
    
    # Start the timer
    start_time = time.time()
    
    # Load ETDs from JSON
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            # Try to determine if it's an array or object
            first_char = f.read(1).strip()
            f.seek(0)  # Reset file pointer
            
            if first_char == '[':
                # JSON array format
                etds = json.load(f)
            elif first_char == '{':
                # JSON object format with items inside
                data = json.load(f)
                # Try to find an array field
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, list) and len(value) > 0:
                            etds = value
                            print(f"Found ETD array in field '{key}'")
                            break
                    else:
                        # If no array field, use the object itself
                        etds = [data]
                else:
                    etds = [data]
            else:
                # Unexpected format, try direct load
                etds = json.load(f)
                if not isinstance(etds, list):
                    etds = [etds]
                    
        # Sample data printing
        print(f"Loaded {len(etds)} ETDs from {json_path}")
        
        # if etds:
        #     print(f"Sample ETD format: {json.dumps(etds[0], indent=2)[:500]}...")
        #     print(f"Available fields: {list(etds[0].keys())}")
            
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        print(f"Exception details: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False
    
    # Load ETDs into Neo4j
    try:
        with driver.session() as session:
            for i, etd in enumerate(etds):
                # Extract properties - using dict.get() to handle missing fields
                # Use the 'id' field directly (not <id>)
                etd_id = etd.get("id", "")
                title = etd.get("title", "Unknown Title")
                uri = etd.get("URI", etd.get("uri", ""))
                author = etd.get("author", "")
                advisor = etd.get("advisor", "")
                year = etd.get("year", "")
                abstract = etd.get("abstract", "")
                university = etd.get("university", "")
                degree = etd.get("degree", "")
                language = etd.get("language", "")
                schooltype = etd.get("schooltype", "")
                oadsclassifier = etd.get("oadsclassifier", "")
                borndigital = etd.get("borndigital", "")
                department = etd.get("department", "")
                discipline = etd.get("discipline", "")
                
                # Skip records with empty titles
                if not title or title == "Unknown Title":
                    print(f"Skipping record {i+1} with missing title")
                    continue
                
                # Create Title node with id and uri properties 
                session.run("""
                    MERGE (t:Title {value: $title})
                    SET t.id = $id,
                        t.uri = $uri
                """, title=title, id=etd_id, uri=uri)
                
                # Create Author node and relationship
                if author:
                    session.run("""
                        MERGE (a:Author {name: $author})
                        WITH a
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:HAS_AUTHOR]->(a)
                    """, author=author, title=title)
                
                # Create Advisor node and relationship
                if advisor:
                    session.run("""
                        MERGE (adv:Advisor {name: $advisor})
                        WITH adv
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:ACADEMIC_ADVISOR]->(adv)
                    """, advisor=advisor, title=title)
                
                # Create Year node and relationship
                if year:
                    session.run("""
                        MERGE (y:Year {value: $year})
                        WITH y
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:PUBLISHED_IN]->(y)
                    """, year=year, title=title)
                
                # Create Abstract node and relationship
                if abstract:
                    session.run("""
                        MERGE (abs:Abstract {text: $abstract})
                        WITH abs
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:HAS_ABSTRACT]->(abs)
                    """, abstract=abstract, title=title)
                
                # Create University node and relationship
                if university:
                    session.run("""
                        MERGE (u:University {name: $university})
                        WITH u
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:PUBLISHED_BY]->(u)
                    """, university=university, title=title)

                # Create Department node and relationship
                if department:
                    session.run("""
                        MERGE (d:Department {name: $department})
                        WITH d
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:ACADEMIC_DEPARMENT]->(d)
                    """, department=department, title=title)

                # Create Discipline node and relationship
                if discipline:
                    session.run("""
                        MERGE (dis:Discipline {name: $discipline})
                        WITH dis
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:ACADEMIC_DISCIPLINE]->(dis)
                    """, discipline=discipline, title=title)
                    
                # Create Degree node and relationship
                if degree:
                    session.run("""
                        MERGE (deg:Degree {name: $degree})
                        WITH deg
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:DEGREE_TYPE]->(deg)
                    """, degree=degree, title=title)
                
                # Create Language node and relationship
                if language:
                    session.run("""
                        MERGE (l:Language {name: $language})
                        WITH l
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:WRITTEN_IN]->(l)
                    """, language=language, title=title)
                
                # Create SchoolType node and relationship
                if schooltype:
                    session.run("""
                        MERGE (st:SchoolType {type: $schooltype})
                        WITH st
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:OF_SCHOOL_TYPE]->(st)
                    """, schooltype=schooltype, title=title)
                
                # Create OadsClassifier node and relationship
                if oadsclassifier:
                    session.run("""
                        MERGE (oc:OadsClassifier {value: $oadsclassifier})
                        WITH oc
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:HAS_CLASSIFICATION]->(oc)
                    """, oadsclassifier=oadsclassifier, title=title)
                
                # Create BornDigital node and relationship
                if borndigital:
                    session.run("""
                        MERGE (bd:BornDigital {value: $borndigital})
                        WITH bd
                        MATCH (t:Title {value: $title})
                        MERGE (t)-[:IS_BORN_DIGITAL]->(bd)
                    """, borndigital=borndigital, title=title)
        
        end_time = time.time()  # End the timer
        elapsed_time = end_time - start_time
        print(f"Completed loading ETDs into Neo4j in {elapsed_time:.2f} seconds")
        return True
        
    except Exception as e:
        print(f"Error loading ETDs into Neo4j: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load ETD metadata into Neo4j")
    parser.add_argument("json_file", help="Path to the JSON file containing ETD metadata")
    parser.add_argument("--clear", action="store_true", help="Clear database before loading")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j connection URI")
    parser.add_argument("--username", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", default="", help="Neo4j password")
    parser.add_argument("--debug", action="store_true", help="Enable additional debug output")
    args = parser.parse_args()
    
    # Enable debug mode if requested
    if args.debug:
        print("Debug mode enabled")
    
    # Update connection if needed
    if args.uri != "bolt://localhost:7687" or args.username != "neo4j" or args.password:
        try:
            driver = GraphDatabase.driver(
                args.uri,
                auth=(args.username, args.password),
                encrypted=False
            )
            print(f"Using custom connection to {args.uri}")
        except Exception as e:
            print(f"Error setting up custom connection: {e}")
            sys.exit(1)
    
    # Check version and connection
    if not check_neo4j_version():
        print("Failed to connect to Neo4j. Please check that Neo4j is running and try again.")
        sys.exit(1)
    
    # Clear database if requested
    if args.clear:
        if not clear_database():
            print("Failed to clear database. Aborting.")
            sys.exit(1)
    
    # Load ETDs
    if not load_etds_from_json(args.json_file):
        print("Failed to load ETDs. Please check the errors above.")
        sys.exit(1)
    
    # Verify load
    # if not verify_load():
    #     print("Load verification failed. The data may not have been loaded correctly.")
    #     sys.exit(1)
        
    print("ETD loading process completed successfully!")