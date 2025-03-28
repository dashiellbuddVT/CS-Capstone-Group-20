# CS-Capstone-Group-20

## ETD (Electronic Theses and Dissertations) Tools

This repository contains tools for working with Electronic Theses and Dissertations (ETDs) data in a Virtuoso triplestore database.

### Components

- **ETDQueries.py**: Query tools for retrieving ETD metadata from the Virtuoso database.
- **ETDLoader.py**: Tool for loading ETD metadata into the Virtuoso database.
- **ETDExplorer.py**: GUI application for browsing and exploring ETDs.
- **test_etd_loader.py**: Testing framework for the ETD loader.

### Known Issues

- **Read-Only Virtuoso Setup**: The Virtuoso database appears to be configured in read-only mode for the provided credentials. While SPARQL INSERT queries receive a 200 OK response, the data is not actually persisted in the database.
- **ETDLoader.py** includes a permission check function that verifies whether write operations are being stored.

### Usage

#### Querying ETDs
```bash
python3 ETDQueries.py
```

#### Loading ETDs (Note: Database is Read-Only)
```bash
python3 ETDLoader.py 
```

#### Testing ETD Loader
```bash
# Run a single test with specified size
python3 test_etd_loader.py --size <number_of_test_etds>

# Run performance benchmark with multiple dataset sizes
python3 test_etd_loader.py --benchmark

# Prevent cleanup of test data after test runs
python3 test_etd_loader.py --size <number_of_test_etds> --no-cleanup
```

The benchmark option tests the loader with multiple dataset sizes (10, 50, 100, 500, 1000 ETDs) and generates a performance graph showing execution time and loading rates.

#### Exploring ETDs (GUI Application)
```bash
python3 ETDExplorer.py
```
