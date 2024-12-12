import pandas as pd
import argparse

def filter_rows_with_phone(file_name):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Ensure the 'Phone' column exists
        if "Phone" not in df.columns:
            print(f"Error: The column 'Phone' does not exist in {file_name}.")
            return

        # Filter rows where 'Phone' is not empty
        filtered_df = df[df["Phone"].notna() & (df["Phone"] != "")]

        # Print the filtered DataFrame
        print("\nRows with a value in the 'Phone' column:")
        print(filtered_df)

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Filter rows with a 'Phone' column value in a CSV file.")
    parser.add_argument("file", help="Path to the input CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided file name
    filter_rows_with_phone(args.file)

