import os
import sys
import argparse
import requests
from requests.auth import HTTPDigestAuth

# Add parent directory to path to import the ETDLoader module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from virtuoso.ETDLoader import send_sparql_query, graph_URI, username, password
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

def cleanup_by_prefix(id_prefix):
    """Delete ETDs from the database based on their ID prefix"""
    print(f"\nCleaning up data with ID prefix '{id_prefix}'...")
    
    # SPARQL query to delete all triples related to ETDs with the given prefix
    delete_query = f"""
    DELETE {{
      ?s ?p ?o .
    }}
    FROM <{graph_URI}>
    WHERE {{
      ?s ?p ?o .
      FILTER(REGEX(STR(?s), "http://erdkb.endeavour.cs.vt.edu/etd/{id_prefix}"))
    }}
    """
    
    try:
        response = send_sparql_query(delete_query)
        
        if response.status_code == 200:
            print(f"Successfully cleaned up data with ID prefix '{id_prefix}'")
            return True
        else:
            print(f"Error cleaning up data: {response.status_code} - {response.text[:200]}")
            return False
    except Exception as e:
        print(f"Exception during cleanup: {str(e)}")
        return False

def clear_all_graph_data():
    """Clear all data from the graph (use with extreme caution)"""
    print(f"\nWARNING: You are about to clear ALL data from the graph {graph_URI}")
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

def main():
    """
    Clean up ETD data from the database by specifying an ID prefix
    """
    parser = argparse.ArgumentParser(description='Clean up ETD data from the database')
    parser.add_argument('--prefix', type=str, default='test', 
                        help='ID prefix of ETDs to delete (default: "test")')
    parser.add_argument('--all', action='store_true',
                        help='Delete all test data including benchmark data (bench prefix)')
    parser.add_argument('--clear-graph', action='store_true',
                        help='Clear the entire graph (DANGER: removes ALL data)')
    
    args = parser.parse_args()
    
    if args.clear_graph:
        success = clear_all_graph_data()
        return success
    
    if args.all:
        # Clean up all test data
        print("Cleaning up all test data...")
        prefixes = ['test', 'bench', 'etd']
        success = True
        for prefix in prefixes:
            if not cleanup_by_prefix(prefix):
                success = False
        return success
    else:
        # Clean up data with specified prefix
        return cleanup_by_prefix(args.prefix)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 