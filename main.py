import pandas as pd
import sys

def remove_columns(input_file, output_file=None):
    # Columns to remove
    columns_to_remove = [
         "Award Title", "Agency", "Branch", "Phase", "Program", 
        "Agency Tracking Number", "Contract", "Proposal Award Date", 
        "Contract End Date", "Solicitation Number", "Solicitation Year", 
        "Solicitation Close Date", "Proposal Receipt Date", "Date of Notification", 
        "Topic Code", "Award Year", "Award Amount", "Duns", "HUBZone Owned", 
        "Socially and Economically Disadvantaged", "Women Owned", "Number Employees", 
        "Company Website", "Address1", "Address2", "City",  "Abstract", 
        "Contact Name", "Contact Title",
        "PI Name", "PI Title",
        "RI Name", "RI POC Name", "RI POC Phone"
    ]

    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Remove specified columns
        columns_to_keep = [col for col in df.columns if col not in columns_to_remove]
        df_filtered = df[columns_to_keep]
        
        # Determine output file name
        if output_file is None:
            output_file = input_file.replace('.csv', '_filtered.csv')
        
        # Save the filtered DataFrame
        df_filtered.to_csv(output_file, index=False)
        
        print(f"Filtered CSV saved to {output_file}")
        print(f"Original columns: {len(df.columns)}")
        print(f"Remaining columns: {len(df_filtered.columns)}")
        print("Remaining columns:", list(df_filtered.columns))
    
    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_csv_file> [output_csv_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    remove_columns(input_file, output_file)

if __name__ == "__main__":
    main()
