import os
import json
import time
import random
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from ETDLoader import load_etds_from_json as sparql_load
from ETDLoader import send_sparql_query, graph_URI
from ETDQueries import get_etd_count

def generate_test_data(num_etds=100, output_file="test_etds.json", id_prefix="test"):
    """Generate test ETD data for benchmarking"""
    print(f"Generating {num_etds} test ETDs...")
    
    # Sample data for ETD generation
    universities = ["Virginia Tech", "University of Virginia", "George Mason University"]
    departments = ["Computer Science", "Biology", "Chemistry", "Mathematics", "Physics"]
    topics = ["Machine Learning", "Quantum Computing", "Blockchain", "Artificial Intelligence"]
    
    # Generate random ETDs
    etds = []
    for i in range(1, num_etds + 1):
        # Generate a random date
        year = random.randint(2000, 2023)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        date = datetime(year, month, day).strftime("%Y-%m-%d")
        
        # Generate author name
        first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer"]
        last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis"]
        author = f"{random.choice(first_names)} {random.choice(last_names)}"
        
        # Create ETD object
        topic = random.choice(topics)
        department = random.choice(departments)
        
        etd = {
            "id": f"{id_prefix}{i}",
            "uri": f"https://example.com/etd/{id_prefix}{i}",
            "title": f"Test ETD {i}: A Study of {topic} in {department}",
            "author": author,
            "university": random.choice(universities),
            "department": department,
            "year": str(year),
            "date": date,
            "abstract": f"This is a test abstract for ETD {i} about {topic}.",
            "keywords": [topic, random.choice(topics)]
        }
        
        etds.append(etd)
    
    # Save to file
    with open(output_file, 'w') as f:
        json.dump(etds, f, indent=2)
    
    print(f"Test data saved to {output_file}")
    return output_file, num_etds

def cleanup_test_data(id_prefix="test"):
    """Delete test ETDs from the database based on their ID prefix"""
    print(f"\nCleaning up test data with ID prefix '{id_prefix}'...")
    
    # SPARQL query to delete all triples related to test ETDs
    delete_query = f"""
    DELETE {{
      ?s ?p ?o .
    }}
    FROM <{graph_URI}>
    WHERE {{
      ?s ?p ?o .
      FILTER(REGEX(STR(?s), "http://localhost:8890/etd/{id_prefix}"))
    }}
    """
    
    response = send_sparql_query(delete_query)
    
    if response.status_code == 200:
        print(f"Successfully cleaned up test data with ID prefix '{id_prefix}'")
        return True
    else:
        print(f"Error cleaning up test data: {response.status_code} - {response.text}")
        return False

def benchmark(test_sizes, cleanup=True):
    """Benchmark SPARQL INSERT method with different dataset sizes"""
    results = {
        "sizes": test_sizes,
        "times": [],
        "rates": []
    }
    
    id_prefix = f"bench{int(time.time())}_"
    
    try:
        for size in test_sizes:
            print(f"\n{'='*50}")
            print(f"BENCHMARKING WITH {size} ETDs")
            print(f"{'='*50}")
            
            # Generate test data
            test_file, _ = generate_test_data(size, f"benchmark_{size}_etds.json", id_prefix=id_prefix)
            
            # Validate the test data
            print("\nValidating test data...")
            try:
                with open(test_file, 'r') as f:
                    etds = json.load(f)
                print(f"JSON is valid. Contains {len(etds)} ETDs.")
                # Check a sample ETD
                if etds:
                    sample = etds[0]
                    print(f"Sample ETD fields: {', '.join(sample.keys())}")
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON in test file: {str(e)}")
                results["times"].append(None)
                results["rates"].append(None)
                continue
            
            # Benchmark SPARQL method
            print("\nTesting SPARQL INSERT method...")
            count_before = get_etd_count()
            print(f"ETD count before: {count_before}")
            
            start_time = time.time()
            try:
                success = sparql_load(test_file, max_batches=None)
                load_time = time.time() - start_time
                count_after = get_etd_count()
                print(f"ETD count after: {count_after}")
                etds_loaded = count_after - count_before
                
                # If no ETDs were loaded but no error was reported, check configuration
                if etds_loaded == 0 and success:
                    print("Warning: Success reported but no ETDs were loaded. Checking configuration...")
                    print(f"Graph URI: {graph_URI}")
                    print(f"Endpoint URL: {endpoint_URL if 'endpoint_URL' in globals() else 'Unknown'}")
                    # Try a simple test query to check connectivity
                    test_query = f"SELECT COUNT(*) as ?count FROM <{graph_URI}> WHERE {{ ?s ?p ?o }}"
                    try:
                        response = send_sparql_query(test_query)
                        print(f"Test query status: {response.status_code}")
                        print(f"Test query response: {response.text[:200]}...")
                    except Exception as e:
                        print(f"Test query failed: {str(e)}")
                
                if success:
                    rate = size / load_time if load_time > 0 else 0
                    results["times"].append(load_time)
                    results["rates"].append(rate)
                    print(f"SPARQL method: {load_time:.2f} seconds, {rate:.2f} ETDs/second")
                    print(f"ETDs loaded: {etds_loaded}")
                else:
                    results["times"].append(None)
                    results["rates"].append(None)
                    print("SPARQL method failed")
            except Exception as e:
                print(f"Error in SPARQL method: {str(e)}")
                import traceback
                traceback.print_exc()
                results["times"].append(None)
                results["rates"].append(None)
            
            # Clean up test file
            if os.path.exists(test_file):
                os.remove(test_file)
            
            # Wait between tests
            time.sleep(5)
    finally:
        # Clean up test data from database
        if cleanup:
            cleanup_test_data(id_prefix)
    
    return results

def plot_results(results):
    """Plot the benchmark results"""
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Plot execution times
    ax1.plot(results["sizes"], results["times"], 'b-o', label='SPARQL INSERT')
    ax1.set_xlabel('Number of ETDs')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.set_title('Execution Time')
    ax1.legend()
    ax1.grid(True)
    
    # Plot loading rates
    ax2.plot(results["sizes"], results["rates"], 'b-o', label='SPARQL INSERT')
    ax2.set_xlabel('Number of ETDs')
    ax2.set_ylabel('Loading Rate (ETDs/second)')
    ax2.set_title('Loading Rate')
    ax2.legend()
    ax2.grid(True)
    
    plt.tight_layout()
    plt.savefig('etd_loader_benchmark.png')
    print("Benchmark results saved to etd_loader_benchmark.png")

def check_etd_exists(etd_id):
    """Check if a specific ETD exists in the database by its ID"""
    print(f"\nChecking if ETD with ID '{etd_id}' exists...")
    
    # Query for the specific ETD by ID
    etd_uri = f"http://localhost:8890/etd/{etd_id}"
    query = f"""
    SELECT ?p ?o FROM <{graph_URI}>
    WHERE {{
        <{etd_uri}> ?p ?o .
    }}
    LIMIT 20
    """
    
    try:
        response = send_sparql_query(query)
        
        if response.status_code == 200:
            bindings = response.json()["results"]["bindings"]
            print(f"Found {len(bindings)} triples for ETD {etd_id}:")
            
            for binding in bindings:
                predicate = binding["p"]["value"]
                obj = binding["o"]["value"]
                print(f"  - {predicate} = {obj}")
                
            return len(bindings) > 0
        else:
            print(f"Query failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Error checking ETD: {str(e)}")
        return False

def run_single_test(num_etds=100, cleanup=True):
    """Run a single test with a specific number of ETDs"""
    print(f"\n{'='*50}")
    print(f"TESTING WITH {num_etds} ETDs")
    print(f"{'='*50}")
    
    # Use a timestamp to create unique ID prefix
    id_prefix = f"test{int(time.time())}_"
    
    # Generate test data
    test_file, _ = generate_test_data(num_etds, f"test_{num_etds}_etds.json", id_prefix=id_prefix)
    
    # Validate the test data
    print("\nValidating test data...")
    try:
        with open(test_file, 'r') as f:
            etds = json.load(f)
        print(f"JSON is valid. Contains {len(etds)} ETDs.")
        # Check a sample ETD
        if etds:
            sample = etds[0]
            print(f"Sample ETD fields: {', '.join(sample.keys())}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in test file: {str(e)}")
        if cleanup and os.path.exists(test_file):
            os.remove(test_file)
        return None
    
    results = {}
    
    try:
        # Test SPARQL method
        print("\nTesting SPARQL INSERT method...")
        count_before = get_etd_count()
        print(f"ETD count before: {count_before}")
        
        start_time = time.time()
        try:
            success = sparql_load(test_file, max_batches=None)
            load_time = time.time() - start_time
            count_after = get_etd_count()
            print(f"ETD count after: {count_after}")
            etds_loaded = count_after - count_before
            
            # Check if anything was loaded
            if etds_loaded == 0:
                print("Warning: No ETDs were loaded.")
                
                # Try to directly check for a specific ETD
                if etds:
                    first_etd_id = etds[0]["id"]
                    etd_exists = check_etd_exists(first_etd_id)
                    if etd_exists:
                        print(f"ETD with ID '{first_etd_id}' was found in the database, but not counted correctly.")
                    else:
                        print(f"ETD with ID '{first_etd_id}' was not found in the database.")
                
                # Try a simple test query to check connectivity
                test_query = f"SELECT COUNT(*) as ?count FROM <{graph_URI}> WHERE {{ ?s ?p ?o }}"
                try:
                    response = send_sparql_query(test_query)
                    print(f"Test query status: {response.status_code}")
                    print(f"Test query response: {response.text[:200]}...")
                except Exception as e:
                    print(f"Test query failed: {str(e)}")
            
            if success:
                rate = num_etds / load_time if load_time > 0 else 0
                results["sparql"] = {
                    "time": load_time,
                    "rate": rate,
                    "etds_loaded": etds_loaded
                }
                print(f"SPARQL method: {load_time:.2f} seconds, {rate:.2f} ETDs/second")
                print(f"ETDs loaded: {etds_loaded}")
            else:
                print("SPARQL method failed")
        except Exception as e:
            print(f"Error in SPARQL method: {str(e)}")
            import traceback
            traceback.print_exc()
    finally:
        # Clean up test data from database if requested
        if cleanup:
            cleanup_test_data(id_prefix)
        
        # Clean up test file
        if os.path.exists(test_file):
            os.remove(test_file)
    
    return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test and benchmark ETD loading')
    parser.add_argument('--benchmark', action='store_true', help='Run full benchmark with multiple sizes')
    parser.add_argument('--size', type=int, default=100, help='Number of ETDs for single test')
    parser.add_argument('--no-cleanup', action='store_true', help='Do not clean up test data after running tests')
    args = parser.parse_args()
    
    # Determine whether to clean up test data
    cleanup = not args.no_cleanup
    
    if args.benchmark:
        # Run benchmark with multiple sizes
        test_sizes = [10, 50, 100, 500, 1000]
        results = benchmark(test_sizes, cleanup=cleanup)
        plot_results(results)
    else:
        # Run single test
        run_single_test(args.size, cleanup=cleanup)

if __name__ == "__main__":
    print("=== ETD Loader Testing Tool ===")
    main()
    print("\n=== Test completed ===") 