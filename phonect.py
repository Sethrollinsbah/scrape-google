import pandas as pd
import argparse

def count_rows_with_phone(file_name):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Ensure the 'Phone' column exists
        if "Phone" not in df.columns:
            print(f"Error: The column 'Phone' does not exist in {file_name}.")
            return

        # Count rows where 'Phone' is not empty
        count = df[df["Phone"].notna() & (df["Phone"] != "")].shape[0]

        print(f"Number of rows with a value in the 'Phone' column: {count}")
    except FileNotFoundError:
        print(f"Error: The file '{file_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Count rows with a 'Phone' column value in a CSV file.")
    parser.add_argument("file", help="Path to the input CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided file name
    count_rows_with_phone(args.file)

