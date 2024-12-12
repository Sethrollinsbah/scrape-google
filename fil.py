import pandas as pd
import argparse

def filter_states(file_name, output_file):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Ensure the 'State' column exists
        if "State" not in df.columns:
            print(f"Error: The column 'State' does not exist in {file_name}.")
            return

        # Dictionary mapping long state names to short codes
        state_mapping = {
            "Florida": "FL", "Georgia": "GA", "South Carolina": "SC", "North Carolina": "NC",
            "Virginia": "VA", "Maryland": "MD", "Tennessee": "TN", "Kentucky": "KY", "Ohio": "OH",
            "Michigan": "MI", "Indiana": "IN", "Illinois": "IL", "Missouri": "MO", "Louisiana": "LA",
            "Texas": "TX", "Oklahoma": "OK", "Kansas": "KS", "Colorado": "CO", "Utah": "UT", "Nevada": "NV"
        }

        # List of short state codes to include
        states_to_include = list(state_mapping.values())

        # Map long names to short names in the 'State' column
        df['State'] = df['State'].apply(lambda x: state_mapping.get(x, x))  # Map long names to short names
        
        # Filter the DataFrame to include only the states in the list (both long and short names)
        filtered_df = df[df['State'].isin(states_to_include)]

        # Print the filtered rows
        print("Filtered rows with the selected states:")
        print(filtered_df)

        # Save the filtered data to the output file
        filtered_df.to_csv(output_file, index=False)
        print(f"\nFiltered data has been saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Filter specific states from the 'State' column of a CSV file.")
    parser.add_argument("-f", "--file", help="Path to the input CSV file", required=True)
    parser.add_argument("-o", "--output", help="Path to the output CSV file", required=True)

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided file name and output file name
    filter_states(args.file, args.output)

