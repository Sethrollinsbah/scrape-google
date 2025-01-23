import argparse
import pandas as pd
import pandas as pd
import numpy as np

def expand_phone_rows(df):
    """
    Convert phone number columns into separate rows while preserving other data,
    keeping all original columns and consolidating phone numbers into a single 'Phone' column.
    Removes trailing '.0' from phone numbers.
    """
    # Find all phone-related columns
    phone_columns = [col for col in df.columns if 'phone' in col.lower()]

    if not phone_columns:
        return df

    # Create a list to store our new rows
    expanded_rows = []

    # Get non-phone columns
    non_phone_cols = [col for col in df.columns if col not in phone_columns]

    # Create column list for output DataFrame
    output_columns = non_phone_cols + ['Phone']

    # Iterate through each original row
    for _, row in df.iterrows():
        has_phone = False
        base_data = {col: row[col] for col in non_phone_cols}

        # For each phone column, create a new row if there's a valid number
        for phone_col in phone_columns:
            phone = str(row[phone_col]).strip()

            # Skip empty or invalid phones
            if phone == '' or phone == 'nan' or pd.isna(phone) or phone is None:
                continue

            # Remove trailing '.0'
            if phone.endswith('.0'):
                phone = phone[:-2]

            # Create a new row with non-phone data and current phone
            new_row = base_data.copy()
            new_row['Phone'] = phone
            expanded_rows.append(new_row)
            has_phone = True

        # If no valid phones found, preserve the original row with empty phone
        if not has_phone:
            base_data['Phone'] = ''
            expanded_rows.append(base_data)

    # Create new DataFrame with the specified columns
    result_df = pd.DataFrame(expanded_rows, columns=output_columns)

    return result_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Expand phone number rows in a CSV file.")
    parser.add_argument("input_file", help="Path to the input CSV file.")
    parser.add_argument("output_file", help="Path to the output CSV file.")
    
    args = parser.parse_args()
    
    try:
        # Read input CSV
        df = pd.read_csv(args.input_file)
        
        # Process the DataFrame
        expanded_df = expand_phone_rows(df)
        
        # Save to output file
        expanded_df.to_csv(args.output_file, index=False)
        print(f"Successfully processed {args.input_file} and saved to {args.output_file}")
        print(f"Number of rows in output: {len(expanded_df)}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
