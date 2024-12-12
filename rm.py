import pandas as pd
import argparse

def remove_columns_from_csv(file_name, output_file):
    # List of columns to remove
    columns_to_remove = [
"Other ZIP Code"]
    try:
        # Load the CSV file
        df = pd.read_csv(file_name)

        # Drop specified columns
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')

        # Save the result to a new CSV file
        df.to_csv(output_file, index=False)

        print(f"Updated CSV file saved to {output_file}")
        print("\nPreview of the updated file:")
        print(df.head())
    except FileNotFoundError:
        print(f"Error: The file '{file_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Remove specific columns from a CSV file.")
    parser.add_argument("file", help="Path to the input CSV file")
    parser.add_argument("output", help="Path to save the output CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided file and output paths
    remove_columns_from_csv(args.file, args.output)

