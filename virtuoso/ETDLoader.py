import json
import os
import time
import requests
from requests.auth import HTTPDigestAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration
endpoint_URL = "https://virtuoso.endeavour.cs.vt.edu/sparql-auth"
graph_URI = "http://erdkb.endeavour.cs.vt.edu/ETDs"
username = "dba"
password = "admin"
batch_size = 100  # Reduced from 1000 to avoid 413 errors

def send_sparql_query(query):
    """Send a SPARQL query to the Virtuoso endpoint"""
    
    # Truncate the query output to avoid showing long abstracts
    debug_query = query
    # If query contains abstract, truncate it for display
    if "abstract" in debug_query:
        abstract_start = debug_query.find("<http://erdkb.endeavour.cs.vt.edu/r/abstract>")
        if abstract_start > 0:
            abstract_end = debug_query.find(".", abstract_start)
            if abstract_end > abstract_start:
                # Take 50 chars of abstract to show as sample
                abbreviated = debug_query[abstract_start:abstract_start+50] + "..." + debug_query[abstract_end:]
                # Replace the portion of the debug query
                debug_query = debug_query[:abstract_start] + abbreviated
    
    print(f"DEBUG - Sending query (first 500 chars):\n{debug_query[:500]}...\n")
    
    headers = {
        "Content-Type": "application/sparql-update; charset=utf-8",
        "Accept": "application/sparql-results+json"
    }

    try:
        response = requests.post(
            endpoint_URL,
            data=query.encode('utf-8'),
            auth=HTTPDigestAuth(username, password),
            headers=headers
        )
        
        print(f"DEBUG - Response status: {response.status_code}")
        print(f"DEBUG - Response headers: {response.headers}")
        
        if response.status_code != 200:
            print(f"DEBUG - Response text: {response.text[:200]}")
        
        return response
    except Exception as e:
        print(f"DEBUG - Exception in send_sparql_query: {str(e)}")
        raise

def create_insert_query(etds):
    """
    Create a SPARQL INSERT query for a batch of ETDs
    
    Args:
        etds: List of ETD dictionaries to insert
        
    Returns:
        SPARQL INSERT query string
    """
    # Use format consistent with DBaccess.py
    query = f"INSERT DATA {{ GRAPH <{graph_URI}> {{"
    
    for etd in etds:
        # URI for the ETD - use format that follows the web server route structure
        etd_uri = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/{etd['id']}"
        
        # Helper function to properly escape text for SPARQL
        def escape_for_sparql(text):
            if not text:
                return ""
            # Handle common escape sequences
            text = text.replace('\\', '\\\\')  
            text = text.replace('"', '\\"')
            text = text.replace('\n', '\\n')
            text = text.replace('\r', '\\r')
            text = text.replace('\t', '\\t')

            # Remove other non-printable characters that could cause issues
            text = ''.join(c for c in text if c.isprintable() or c in ['\n', '\r', '\t'])
            return text
        
        # Set up predicates with the proper URL format
        title_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasTitle"
        author_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasAuthor"
        year_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/issuedDate"
        uri_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/identifier"
        abstract_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasAbstract"
        department_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/academicDiscipline"
        discipline_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/academicDiscipline"
        university_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/publishedBy"
        keyword_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasKeyword"
        hasAuthor_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasAuthor"
        hasChapter_predicate = f"http://etdkb.endeavour.cs.vt.edu/v1/predicate/hasPart"
        
        # Safely escape and format the title
        title = escape_for_sparql(etd['title'])
        query += f"\n<{etd_uri}> <{title_predicate}> \"{title}\" ."
        
        author = escape_for_sparql(etd['author'])
        query += f"\n<{etd_uri}> <{author_predicate}> \"{author}\" ."
        # Add author as object reference too
        author_obj = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/author{etd['id']}"
        query += f"\n<{etd_uri}> <{hasAuthor_predicate}> <{author_obj}> ."
        
        query += f"\n<{etd_uri}> <{year_predicate}> \"{etd['year']}\" ."
        
        # Add URI if available
        if 'URI' in etd and etd['URI']:
            uri = escape_for_sparql(etd['URI'])
            query += f"\n<{etd_uri}> <{uri_predicate}> \"{uri}\" ."
        
        # Additional metadata if available
        if 'abstract' in etd and etd['abstract']:
            abstract = escape_for_sparql(etd['abstract'])
            query += f"\n<{etd_uri}> <{abstract_predicate}> \"{abstract}\" ."
        
        if 'department' in etd and etd['department']:
            department = escape_for_sparql(etd['department']).replace(' ','-')
            department_obj = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/{department}"
            query += f"\n<{etd_uri}> <{department_predicate}> \"{department}\" ."
            query += f"\n<{etd_uri}> <{hasChapter_predicate}> <{department_obj}> ."
        
        if 'discipline' in etd and etd['discipline']:
            discipline = escape_for_sparql(etd['discipline']).replace(' ','-')
            discipline_obj = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/{discipline}"
            query += f"\n<{etd_uri}> <{discipline_predicate}> \"{discipline}\" ."
            query += f"\n<{etd_uri}> <{hasChapter_predicate}> <{discipline_obj}> ."
        
        if 'university' in etd and etd['university']:
            university = escape_for_sparql(etd['university']).replace(' ','-')
            university_obj = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/{university}"
            query += f"\n<{etd_uri}> <{university_predicate}> \"{university}\" ."
            query += f"\n<{etd_uri}> <{hasChapter_predicate}> <{university_obj}> ."
        
        # Keywords as separate triples
        if 'keywords' in etd and etd['keywords']:
            for i, keyword in enumerate(etd['keywords']):
                if keyword:
                    keyword = escape_for_sparql(keyword)
                    query += f"\n<{etd_uri}> <{keyword_predicate}> \"{keyword}\" ."
                    # Add keyword as object reference too
                    keyword_obj = f"http://etdkb.endeavour.cs.vt.edu/v1/objects/{keyword.replace(' ', '-')}"
                    query += f"\n<{etd_uri}> <{hasChapter_predicate}> <{keyword_obj}> ."
    
    # Close the query - exactly two closing braces, no more
    query += "\n}}"
    
    return query

def load_batch(batch, batch_num=None):
    """
    Load a batch of ETDs into the database
    
    Args:
        batch: List of ETD dictionaries
        batch_num: Batch number (for logging)
        
    Returns:
        Tuple of (success, count) where:
            success: True if the batch was loaded successfully
            count: Number of ETDs in the batch
    """
    try:
        count = len(batch)
        if batch_num is not None:
            print(f"Processing batch {batch_num} with {count} ETDs...")
        
        query = create_insert_query(batch)
        query_size = len(query.encode('utf-8'))
        print(f"Batch {batch_num} query size: {query_size/1024:.2f} KB")
        
        response = send_sparql_query(query)
        
        if response.status_code == 200:
            print(f"Batch {batch_num} loaded successfully")
            return True, count
        else:
            print(f"Error loading batch {batch_num}: {response.status_code} - {response.text[:500]}")
            return False, 0
    except Exception as e:
        print(f"Exception in batch {batch_num}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, 0

def load_etds_from_json(json_file_path, max_batches=None, num_workers=4):
    """
    Load ETDs from a JSON file into the database
    
    Args:
        json_file_path: Path to the JSON file
        max_batches: Maximum number of batches to load (None for all)
        num_workers: Number of parallel workers for batch loading
        
    Returns:
        True if loading was successful, False otherwise
    """
    try:
        start_time = time.time()
        print(f"Loading ETDs from {json_file_path}...")
        
        # Read JSON data
        with open(json_file_path, 'r') as f:
            etds = json.load(f)
        
        if not isinstance(etds, list):
            print(f"Error: Expected a list of ETDs, got {type(etds)}")
            return False
        
        total_etds = len(etds)
        print(f"Found {total_etds} ETDs to load")
        
        # Split into batches
        batches = []
        for i in range(0, total_etds, batch_size):
            batches.append(etds[i:i+batch_size])
        
        total_batches = len(batches)
        print(f"Split into {total_batches} batches of up to {batch_size} ETDs each")
        
        # Limit number of batches if requested
        if max_batches is not None and max_batches < total_batches:
            batches = batches[:max_batches]
            print(f"Limited to {max_batches} batches as requested")
        
        total_loaded = 0
        success_count = 0
        failed_batches = []
        
        # Process batches in parallel
        print(f"Processing batches with {num_workers} parallel workers...")
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            
            futures = {executor.submit(load_batch, batch, i+1): i+1 for i, batch in enumerate(batches)}
            
            # Process as they complete
            with tqdm(total=len(futures), desc="Loading ETDs") as progress:
                for future in as_completed(futures):
                    batch_num = futures[future]
                    try:
                        success, count = future.result()
                        if success:
                            success_count += 1
                            total_loaded += count
                        else:
                            failed_batches.append(batch_num)
                        progress.update(1)
                    except Exception as e:
                        print(f"Batch {batch_num} failed: {str(e)}")
                        failed_batches.append(batch_num)
                        progress.update(1)
        
        # Calculate statistics
        elapsed_time = time.time() - start_time
        batches_processed = len(batches)
        
        print(f"\nLoading completed in {elapsed_time:.2f} seconds")
        print(f"Successfully loaded {total_loaded} ETDs ({success_count}/{batches_processed} batches)")
        
        if failed_batches:
            failed_batches.sort()
            print(f"Failed batches: {failed_batches}")
        
        if elapsed_time > 0:
            print(f"Average rate: {total_loaded/elapsed_time:.2f} ETDs per second")
        
        # Return success if all batches were processed successfully
        return success_count == batches_processed
    
    except Exception as e:
        print(f"Error in ETD loading process: {str(e)}")
        return False

def main():
    """Main function for command-line usage"""
    import argparse
    
    # Add a warning about write operations
    print("\n" + "="*80)
    print("WARNING: This system appears to be in read-only mode.")
    print("While SPARQL INSERT queries are accepted by the server (status 200),")
    print("the data is not being persisted in the database.")
    
    parser = argparse.ArgumentParser(description='Load ETD metadata into Virtuoso')
    parser.add_argument('json_file', help='Path to the JSON file containing ETD metadata')
    parser.add_argument('--max-batches', type=int, help='Maximum number of batches to load')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--force', action='store_true', help='Force loading even if write permission check fails')
    args = parser.parse_args()
    
    if not os.path.exists(args.json_file):
        print(f"Error: JSON file not found: {args.json_file}")
        return False
    
    return load_etds_from_json(args.json_file, args.max_batches, args.workers)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 