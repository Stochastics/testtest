import pandas as pd
import os

def safe_sort(values):
    """Safely sort a list of mixed types by converting all to strings"""
    try:
        return sorted(list(values), key=str)
    except:
        return list(values)  # Return unsorted if sorting fails

def compare_dataframes(df1, df2, unique_id_col):
    """
    Compare two dataframes and show high-level differences plus record-level changes
    """
    report = {}
    
    # Basic count comparisons
    report['counts'] = {
        'df1_total_records': len(df1),
        'df2_total_records': len(df2),
        'df1_unique_ids': df1[unique_id_col].nunique(),
        'df2_unique_ids': df2[unique_id_col].nunique()
    }
    
    # Column comparisons
    columns_df1 = set(df1.columns)
    columns_df2 = set(df2.columns)
    
    report['columns'] = {
        'only_in_df1': list(columns_df1 - columns_df2),
        'only_in_df2': list(columns_df2 - columns_df1),
        'value_differences': {}
    }
    
    # For common columns, compare unique values
    common_columns = columns_df1.intersection(columns_df2)
    for col in common_columns:
        # Convert to strings to handle mixed types
        values_df1 = set(df1[col].astype(str).dropna().unique())
        values_df2 = set(df2[col].astype(str).dropna().unique())
        
        if values_df1 != values_df2:
            report['columns']['value_differences'][col] = {
                'df1_values': safe_sort(values_df1),
                'df2_values': safe_sort(values_df2),
                'only_in_df1': safe_sort(values_df1 - values_df2),
                'only_in_df2': safe_sort(values_df2 - values_df1)
            }
    
    # Find records with differences
    common_ids = set(df1[unique_id_col]).intersection(set(df2[unique_id_col]))
    record_differences = []
    
    for id_val in common_ids:
        row1 = df1[df1[unique_id_col] == id_val].iloc[0]
        row2 = df2[df2[unique_id_col] == id_val].iloc[0]
        
        differences = {}
        for col in common_columns:
            val1 = str(row1[col])
            val2 = str(row2[col])
            if val1 != val2:
                differences[col] = {'df1': val1, 'df2': val2}
        
        if differences:
            for col, vals in differences.items():
                record_differences.append({
                    'Record ID': id_val,
                    'Column': col,
                    'DF1 Value': vals['df1'],
                    'DF2 Value': vals['df2']
                })
    
    report['record_differences'] = record_differences
    return report, pd.DataFrame(record_differences)

def print_report(report):
    """Print the comparison report in a readable format and export to CSV files"""
    print("\n=== RECORD COUNTS ===")
    print(f"DataFrame 1: {report['counts']['df1_total_records']} records ({report['counts']['df1_unique_ids']} unique IDs)")
    print(f"DataFrame 2: {report['counts']['df2_total_records']} records ({report['counts']['df2_unique_ids']} unique IDs)")
    
    # Export record counts to CSV
    counts_df = pd.DataFrame([report['counts']])
    counts_df.to_csv('reports/record_counts.csv', index=False)
    print("\nRecord counts exported to 'record_counts.csv'")
    
    print("\n=== COLUMN DIFFERENCES ===")
    if report['columns']['only_in_df1']:
        print(f"Columns only in DataFrame 1: {', '.join(report['columns']['only_in_df1'])}")
    if report['columns']['only_in_df2']:
        print(f"Columns only in DataFrame 2: {', '.join(report['columns']['only_in_df2'])}")
    
    # Export column differences to CSV
    columns_df = pd.DataFrame({
        'only_in_df1': [', '.join(report['columns']['only_in_df1'])],
        'only_in_df2': [', '.join(report['columns']['only_in_df2'])]
    })
    columns_df.to_csv('reports/column_differences.csv', index=False)
    print("Column differences exported to 'column_differences.csv'")
    
    if report['columns']['value_differences']:
        print("\n=== DIFFERENT VALUES IN COLUMNS ===")
        for col, diff in report['columns']['value_differences'].items():
            print(f"\nColumn: {col}")
            print(f"Values only in DataFrame 1: {diff['only_in_df1']}")
            print(f"Values only in DataFrame 2: {diff['only_in_df2']}")
        
        # Export value differences to CSV
        value_differences_list = []
        for col, diff in report['columns']['value_differences'].items():
            value_differences_list.append({
                'Column': col,
                'Values only in DataFrame 1': ', '.join(diff['only_in_df1']),
                'Values only in DataFrame 2': ', '.join(diff['only_in_df2'])
            })
        value_differences_df = pd.DataFrame(value_differences_list)
        value_differences_df.to_csv('reports/value_differences.csv', index=False)
        print("Value differences exported to 'value_differences.csv'")

# Example usage:
try:
    # Add low_memory=False to handle mixed types
    df1 = pd.read_csv('prod_dec23.csv', encoding='latin1', low_memory=False,dtype={"InspectorDistrict": "Int64"})
    df2 = pd.read_csv('test_dec23.csv', encoding='latin1', low_memory=False,dtype={"InspectorDistrict": "Int64"})

    # Convert float columns that only contain integers to Int64 (nullable integer type)
    for col in df1.select_dtypes(include=['float']).columns:
        if df1[col].dropna().mod(1).eq(0).all():  # Check if all values are whole numbers
            df1[col] = df1[col].astype("Int64")

    for col in df2.select_dtypes(include=['float']).columns:
        if df2[col].dropna().mod(1).eq(0).all():
            df2[col] = df2[col].astype("Int64")

except UnicodeDecodeError as e:
    print(f"Error reading CSV files: {e}")
    print("Try using a different encoding, such as 'ISO-8859-1' or 'utf-16'.")
    exit()

# Compare dataframes and print rzeport
report, differences_df = compare_dataframes(df1, df2, 'RowID')  # Replace 'RowID' with your unique ID column name
print_report(report)

# Export record differences to a CSV file
differences_df.to_csv('reports/record_differences.csv', index=False)
print("\nRecord differences exported to 'record_differences.csv'")
