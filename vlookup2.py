import csv

def read_csv_to_dict(file_path, key_column):
    """
    Reads a CSV file into a dictionary where the key is the value from the key_column and the value is the entire row.

    Parameters:
    - file_path: Path to the CSV file.
    - key_column: The column to use as the key for the dictionary.

    Returns:
    - A dictionary representing the CSV file.
    """
    result_dict = {}
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            result_dict[row[key_column]] = row
    return result_dict

def vlookup(input_dict, lookup_value, lookup_column, return_column):
    """
    Performs a VLOOKUP-like operation on a dictionary.

    Parameters:
    - input_dict: The dictionary to operate on.
    - lookup_value: The value to look for.
    - lookup_column: The column to search for the lookup_value.
    - return_column: The column from which to return the value.

    Returns:
    - The corresponding value from the return_column or None if not found.
    """
    for key, row in input_dict.items():
        if row[lookup_column] == lookup_value:
            return row[return_column]
    return None

def replace_column(row, target_column, find_value, replace_value):
    breakpoint()
    """
    Replaces the value in a target column if it matches the find_value.

    Parameters:
    - row: The row dictionary to modify.
    - target_column: The column to check and replace value.
    - find_value: The value to find.
    - replace_value: The value to replace with.

    Returns:
    - The modified row.
    """

    if row[target_column] == find_value:
        row[target_column] = replace_value
    return row

def main(source_id, destination_id, destination_external_id, source_file, destination_file, result_file):
    # File paths
    file1_path = source_file
    file2_path = destination_file
    output_path = result_file

    # Read CSV files into dictionaries
    file1_dict = read_csv_to_dict(file1_path, source_id)
    file2_dict = read_csv_to_dict(file2_path, destination_external_id)

    # Open the output file
    with open(output_path, mode='w', newline='') as output_file:
        writer = csv.writer(output_file)

        # Write the header row
        header = list(file1_dict[next(iter(file1_dict))].keys())
        writer.writerow(header)

        # Perform VLOOKUP for each row in file1 and write the result to the output file
        for key, row in file1_dict.items():
            lookup_result = vlookup(file2_dict, row[source_id], destination_external_id, destination_id)
            modified_row = replace_column(row, source_id, row[source_id], lookup_result)
            writer.writerow(modified_row.values())

    print(f"VLOOKUP operation completed. Results saved to '{output_path}'.")

# if __name__ == '__main__':
#     main('ID', 'ID', 'ExtenalId', 'data/file1.csv', 'data/file2.csv', 'data/result.csv')
