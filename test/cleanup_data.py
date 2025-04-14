import os
import sys
import argparse
import requests
from requests.auth import HTTPDigestAuth

# Add parent directory to path to import the ETDLoader module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from virtuoso.ETDLoader import send_sparql_query, graph_URI, username, password, endpoint_URL
except ImportError:
    # Fallback if virtuoso module is not found
    print("Warning: Could not import from virtuoso.ETDLoader - using default settings")
    endpoint_URL = "https://virtuoso.endeavour.cs.vt.edu/sparql-auth"
    graph_URI = "http://erdkb.endeavour.cs.vt.edu/ETDs"
    username = "dba"
    password = "admin"
    
    def send_sparql_query(query):
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

def clear_all_graph_data(force=False):
    """Clear all data from the graph"""
    print(f"\nClearing ALL data from the graph {graph_URI}")
    
    if not force:
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Operation cancelled")
            return False
    
    try:
        # SPARQL query to clear the entire graph
        clear_query = f"CLEAR GRAPH <{graph_URI}>"
        response = send_sparql_query(clear_query)
        
        if response.status_code == 200:
            print(f"Successfully cleared all data from graph {graph_URI}")
            return True
        else:
            print(f"Error clearing graph: {response.status_code} - {response.text[:200]}")
            return False
    except Exception as e:
        print(f"Exception clearing graph: {str(e)}")
        return False

def cleanup_specific_predicate(predicate):
    """Delete all relationships with a specific predicate"""
    print(f"\nCleaning up all relationships with predicate '{predicate}'...")
    
    # SPARQL query to delete all relationships with the given predicate
    delete_query = f"""
    DELETE {{
      ?s <{predicate}> ?o .
    }}
    FROM <{graph_URI}>
    WHERE {{
      ?s <{predicate}> ?o .
    }}
    """
    
    print(f"Query: {delete_query}")
    
    try:
        response = send_sparql_query(delete_query)
        
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text[:200]}")
        
        if response.status_code == 200:
            print(f"Successfully cleaned up all relationships with predicate '{predicate}'")
            return True
        else:
            print(f"Error cleaning up relationships: {response.status_code} - {response.text[:200]}")
            return False
    except Exception as e:
        print(f"Exception during cleanup: {str(e)}")
        return False

def main():
    """
    Clean up ETD data from the database
    """
    parser = argparse.ArgumentParser(description='Clean up ETD data from the database')
    parser.add_argument('--force', action='store_true',
                        help='Clear all data without confirmation')
    parser.add_argument('--predicate', type=str,
                        help='Clear only relationships with a specific predicate')
    
    args = parser.parse_args()
    
    if args.predicate:
        return cleanup_specific_predicate(args.predicate)
    else:
        return clear_all_graph_data(force=args.force)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 