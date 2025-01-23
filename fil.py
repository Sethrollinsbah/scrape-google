import pandas as pd
import argparse

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Filter specific states from the 'State' column of a CSV file.")
    parser.add_argument("-f", "--file", help="Path to the input CSV file", required=True)
    parser.add_argument("-o", "--output", help="Path to the output CSV file", required=True)

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided file name and output file name
    filter_states(args.file, args.output)

