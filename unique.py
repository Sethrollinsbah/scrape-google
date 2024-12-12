import pandas as pd
import argparse

def print_unique_states(file_name="louisiana.csv"):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Ensure the 'State' column exists
        if "State" not in df.columns:
            print(f"Error: The column 'State' does not exist in {file_name}.")
            return

        # Get unique values in the 'State' column
        unique_states = df["State"].dropna().unique()

        # Print unique states
        print("Unique values in the 'State' column:")
        for state in unique_states:
            print(state)

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Print unique values in the 'State' column of a CSV file.")

    # Parse the arguments

    # Call the function with the provided file name
