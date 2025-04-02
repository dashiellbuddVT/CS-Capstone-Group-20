def main():
    import argparse
    import pandas as pd

    print("here")
    parser = argparse.ArgumentParser(description='convert CSV of metadata into JSON')
    parser.add_argument('CSV_file', help='Path to the csv file containing ETD metadata')
    parser.add_argument('--out_file', type=str, help='json file name')
    args = parser.parse_args()

    csv_file = pd.DataFrame(pd.read_csv(args.CSV_file, sep = ",", header = 0, index_col = False))
    csv_file.to_json(args.out_file, orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
    return 1

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 