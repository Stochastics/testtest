import pandas as pd

def validate_tables(old_table: pd.DataFrame, new_table: pd.DataFrame, id_column: str) -> dict:
    """
    Compares two tables and generates a validation report.

    Args:
        old_table (pd.DataFrame): The original table.
        new_table (pd.DataFrame): The new table to validate.
        id_column (str): The name of the ID column used for matching records.

    Returns:
        dict: A validation report containing record counts and mismatches.
    """
    validation_report = {
        "record_count_old": len(old_table),
        "record_count_new": len(new_table),
        "record_count_match": len(old_table) == len(new_table),
        "mismatched_records": []
    }

    # Ensure the ID column exists in both tables
    if id_column not in old_table.columns or id_column not in new_table.columns:
        raise ValueError(f"ID column '{id_column}' not found in one or both tables.")

    # Merge tables on the ID column to compare records
    merged = old_table.merge(new_table, on=id_column, suffixes=('_old', '_new'), how='outer', indicator=True)

    # Find mismatched records
    mismatched_records = merged[merged['_merge'] != 'both']
    for _, row in mismatched_records.iterrows():
        record_id = row[id_column]
        status = "Missing in new table" if row['_merge'] == 'left_only' else "Missing in old table"
        validation_report["mismatched_records"].append({
            "id": record_id,
            "status": status
        })

    # Compare fields for matching records
    matching_records = merged[merged['_merge'] == 'both']
    for _, row in matching_records.iterrows():
        record_id = row[id_column]
        mismatched_fields = []
        for col in old_table.columns:
            if col != id_column and row[f"{col}_old"] != row[f"{col}_new"]:
                mismatched_fields.append({
                    "field": col,
                    "old_value": row[f"{col}_old"],
                    "new_value": row[f"{col}_new"]
                })
        if mismatched_fields:
            validation_report["mismatched_records"].append({
                "id": record_id,
                "status": "Field mismatch",
                "mismatched_fields": mismatched_fields
            })

    return validation_report


# Example usage
if __name__ == "__main__":
    # Sample data for demonstration
    old_table = pd.DataFrame({
        'ID': [1, 2, 3],
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35]
    })

    new_table = pd.DataFrame({
        'ID': [1, 2, 4],
        'Name': ['Alice', 'Bobby', 'David'],
        'Age': [25, 30, 40]
    })

    # Validate tables
    report = validate_tables(old_table, new_table, id_column='ID')

    # Print the validation report
    print("Validation Report:")
    print(f"Old table record count: {report['record_count_old']}")
    print(f"New table record count: {report['record_count_new']}")
    print(f"Record counts match: {report['record_count_match']}")
    print("\nMismatched Records:")
    for record in report['mismatched_records']:
        print(f"ID: {record['id']}, Status: {record['status']}")
        if 'mismatched_fields' in record:
            for field in record['mismatched_fields']:
                print(f"  Field: {field['field']}, Old Value: {field['old_value']}, New Value: {field['new_value']}")
