import csv
import json
import argparse
import sys
import os

def convert_csv_to_json(csv_path, json_path):
    """
    Convert a CSV file to a JSON file with minimal output
    """
    try:
        # Define the output fields structure
        required_fields = [
            "id", "title", "author", "advisor", "year", 
            "abstract", "university", "degree", "URI", 
            "department", "discipline"
        ]
        
        # Check if the file exists
        if not os.path.exists(csv_path):
            print(f"Error: CSV file '{csv_path}' does not exist")
            return False
            
        # Read CSV file
        etds = []
        with open(csv_path, 'r', encoding='utf-8-sig') as csvfile:
            # Read with DictReader
            reader = csv.DictReader(csvfile, delimiter=',')
            
            # Process all rows
            for row in reader:
                # Create a new dict with required fields (initialized as empty)
                filtered_row = {field: "" for field in required_fields}
                
                # Copy values from CSV row to our dict
                for field in row.keys():
                    if field in required_fields:
                        # Clean empty values and whitespace
                        value = row[field].strip() if row[field] else ""
                        filtered_row[field] = value
                
                # Skip empty rows (rows without id or title)
                if not filtered_row["id"] and not filtered_row["title"]:
                    continue
                    
                # Ensure degree, department, and discipline are empty strings
                filtered_row["degree"] = ""
                filtered_row["department"] = ""
                filtered_row["discipline"] = ""
                
                etds.append(filtered_row)
        
        # Check if we have any data
        if not etds:
            print("Error: No data was extracted from the CSV file")
            return False
            
        # Write JSON file
        with open(json_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(etds, jsonfile, indent=2)
        
        print(f"Successfully converted {len(etds)} ETDs to {json_path}")
        return True
    
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert ETD CSV to JSON with specific fields")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("json_file", help="Path for the output JSON file")
    args = parser.parse_args()
    
    if not convert_csv_to_json(args.csv_file, args.json_file):
        sys.exit(1)
    
    print("Conversion completed successfully!")
