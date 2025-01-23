import pandas as pd
import os
import sys
from typing import Dict, List
import re


def get_column_mapping_regex() -> Dict[str, str]:
    """Define regex-based mapping from column patterns to standardized names."""
    return {
        r'(?i)(full\s*name|contact\s*name|labeler\s*name|legal\s*contact\s*name|tc\s*name|pi\s*name|ri\s*poc\s*name)': ['first_name', 'last_name'],
        r'(?i)(first\s*name)': 'first_name',
        r'(?i)(last\s*name)': 'last_name',
        r'(?i)(company\s*name|business\s*name)': 'business_name',
        r'(?i)(mobile\s*phone|cell\s*phone|smartphone|cellular|phone|contact\s*phone|business\s*phone|company\s*phone|legal\s*phone|invoice\s*phone|tc\s*phone|pi\s*phone|ri\s*poc\s*phone)': 'phone',
        r'(?i)(state|business\s*state|legal\s*state|invoice\s*state|tc\s*state|mailing\s*state|company\s*state)': 'state',
        r'(?i)(zip|zipcode|postal\s*code|business\s*zip|legal\s*zip|invoice\s*zip|tc\s*zip|mailing\s*zip)': 'zip',
        r'(?i)(email|email\s*address|contact\s*email|pi\s*email)': 'email'
    }

def match_column_with_regex(column: str, column_mapping: Dict[str, str]) -> str:
    """Match a column name to its standardized field using regex."""
    for pattern, target in column_mapping.items():
        if re.match(pattern, column):
            return target
    return None

def split_full_name(df: pd.DataFrame, full_name_col: str) -> pd.DataFrame:
    """Split full name into first and last name."""
    if full_name_col in df.columns:
        try:
            df[['first_name', 'last_name']] = df[full_name_col].str.split(' ', n=1, expand=True)
        except:
            df['first_name'] = df[full_name_col]
            df['last_name'] = ''
        df = df.drop(columns=[full_name_col])
    return df

def expand_phone_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create separate rows for each phone number while duplicating other data.
    Returns a DataFrame with one row per phone number.
    """
    if 'phone' not in df.columns:
        return df
    
    # Create list to store expanded rows
    expanded_rows = []
    
    for _, row in df.iterrows():
        phones = []
        base_data = row.drop('phone').to_dict()
        
        # Split multiple phone numbers if they exist in the same field
        potential_phones = str(row['phone']).split(',')
        
        # Process each potential phone number
        for phone in potential_phones:
            if is_valid_phone(phone):
                cleaned_phone = clean_phone(phone)
                if cleaned_phone and cleaned_phone not in phones:
                    phones.append(cleaned_phone)
        
        # If no valid phones found, create one row with empty phone
        if not phones:
            base_data['phone'] = ''
            expanded_rows.append(base_data)
        else:
            # Create a new row for each unique phone number
            for phone in phones:
                new_row = base_data.copy()
                new_row['phone'] = phone
                expanded_rows.append(new_row)
    
    # Create new DataFrame with expanded rows
    result_df = pd.DataFrame(expanded_rows)
    
    return result_df

def expand_phone_rows(df):
    """
    Convert phone number columns into separate rows while preserving other data.
    """
    # Find all phone-related columns
    phone_columns = [col for col in df.columns if 'phone' in col.lower()]
    
    if not phone_columns:
        return df
        
    # Create a list to store our new rows
    expanded_rows = []
    
    # Iterate through each original row
    for _, row in df.iterrows():
        has_phone = False
        
        # For each phone column, create a new row if there's a valid number
        for phone_col in phone_columns:
            phone = str(row[phone_col]).strip()
            
            # Skip empty or invalid phones
            if phone == '' or phone == 'nan' or pd.isna(phone):
                continue
                
            # Create a new row with all original data
            new_row = row.copy()
            
            # Clear all phone columns
            for pc in phone_columns:
                new_row[pc] = ''
                
            # Set the current phone number in its original column
            new_row[phone_col] = phone
            
            expanded_rows.append(new_row)
            has_phone = True
        
        # If no valid phones were found, preserve the original row
        if not has_phone:
            expanded_rows.append(row)
    
    # Convert back to DataFrame
    result_df = pd.DataFrame(expanded_rows)
    
    # Preserve original column order
    result_df = result_df[df.columns]
    
    return result_df

def process_directory(input_dir: str, output_file: str):
    """Process all CSV files in a directory and combine them into one normalized CSV."""
    if not os.path.exists(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)
    
    if not os.path.isdir(input_dir):
        print(f"Error: '{input_dir}' is not a directory.")
        sys.exit(1)
    
    all_dfs = []
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        sys.exit(1)
        
    column_mapping = get_column_mapping_regex()
    
    for filename in csv_files:
        input_path = os.path.join(input_dir, filename)
        print(f"Processing {filename}...")
        
        try:
            # Read CSV
            df = pd.read_csv(input_path)
            
            # First expand the phone rows while keeping original column structure
            df = expand_phone_rows(df)
            
            # Create normalized DataFrame
            normalized_df = pd.DataFrame(columns=[
                'first_name', 'last_name', 'business_name', 'phone',
                'state', 'zip', 'email'
            ])
            
            # Process each column
            for col in df.columns:
                target = match_column_with_regex(col, column_mapping)
                if target:
                    if isinstance(target, list):
                        df = split_full_name(df, col)
                        for i, target_col in enumerate(target):
                            normalized_df[target_col] = df.get(target_col, '')
                    else:
                        if target == 'phone' and str(df[col].iloc[0]).strip() != '':
                            normalized_df['phone'] = df[col].apply(clean_phone)
                        elif target != 'phone':
                            normalized_df[target] = df[col]
            
            all_dfs.append(normalized_df)
        
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.fillna('')
        
        # Clean zip codes
        final_df['zip'] = final_df['zip'].apply(clean_zip)
        
        # Remove rows without valid phone numbers
        initial_rows = len(final_df)
        final_df = final_df[final_df['phone'].str.len() >= 10]
        removed_rows = initial_rows - len(final_df)
        print(f"Removed {removed_rows} rows without valid phone numbers")
        
        # Create output directory if needed
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        try:
            final_df.to_csv(output_file, index=False)
            print(f"Normalized data saved to {output_file}")
            print(f"Final number of rows: {len(final_df)}")
        except Exception as e:
            print(f"Error saving output file: {str(e)}")
            sys.exit(1)
    else:
        print("No data was processed successfully.")
        sys.exit(1)
def is_valid_phone(phone_str: str) -> bool:
    """
    Validate phone number format.
    Valid formats: 10 digits (standard US number) or 11 digits starting with 1
    """
    digits = re.sub(r'\D', '', str(phone_str))
    if len(digits) == 10:
        return True
    if len(digits) == 11 and digits.startswith('1'):
        return True
    return False

def clean_phone(phone) -> str:
    """Clean and validate phone numbers."""
    if pd.isna(phone):
        return ''
    
    # Convert to string and remove non-digits
    digits = re.sub(r'\D', '', str(phone))
    
    # If it's 11 digits starting with 1, remove the 1
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    
    # Return only if it's exactly 10 digits
    return digits if len(digits) == 10 else ''

def filter_states(file_name, output_file):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Ensure the 'State' column exists
        if "state" not in df.columns:
            print(f"Error: The column 'state' does not exist in {file_name}.")
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
        df['state'] = df['state'].apply(lambda x: state_mapping.get(x, x))  # Map long names to short names
        
        # Filter the DataFrame to include only the states in the list (both long and short names)
        filtered_df = df[df['state'].isin(states_to_include)]

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



def clean_zip(zip_code) -> str:
    """Clean zip codes."""
    if pd.isna(zip_code):
        return ''
    
    # Convert to string and remove decimal point and trailing zeros
    zip_str = str(zip_code).split('.')[0]
    
    # Remove non-digits
    digits = re.sub(r'\D', '', zip_str)
    
    # If it's too long (phone-like) or not a valid zip, clear it
    if len(digits) >= 10:
        return ''
        
    # Return first 5 digits if we have them, otherwise an empty string
    return digits[:5] if digits else ''

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python format.py <input_directory> <output_file>")
        sys.exit(1)
    
    input_directory = sys.argv[1]
    output_file = sys.argv[2]
    
    process_directory(input_directory, output_file)
    filter_states('out.csv', 'out.csv')
