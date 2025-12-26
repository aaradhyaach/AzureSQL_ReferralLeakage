import glob
import os

def count_rows_in_csv_files(directory_path):
    # Change to the specified directory
    os.chdir(directory_path)
    # Get a list of all CSV files in the directory
    csv_files = glob.glob('*.csv')
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        return

    print(f"Row counts for CSV files in '{directory_path}' (excluding header count):")
    for filename in csv_files:
        with open(filename, 'r', encoding="utf-8") as f:
            # Count lines efficiently (subtract 1 for header)
            row_count = sum(1 for line in f) - 1
            print(f"* {filename}: {row_count}")

# Replace '/path/to/your/folder' with the actual path
count_rows_in_csv_files('./data/csv')


# Row counts for CSV files in './data/csv' (excluding header count):
# * medications.csv: 56430
# * providers.csv: 5056
# * payer_transitions.csv: 53101
# * imaging_studies.csv: 151637
# * supplies.csv: 1573
# * payers.csv: 10
# * claims.csv: 117889
# * allergies.csv: 794
# * procedures.csv: 83823
# * organizations.csv: 1127
# * conditions.csv: 38094
# * careplans.csv: 3931
# * encounters.csv: 61459
# * devices.csv: 89
# * immunizations.csv: 17009
# * claims_transactions.csv: 711238
# * patients.csv: 1163
# * observations.csv: 531144