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
            print(return_column)
            return row[return_column]
    return None

def replaceColumn(row,target_column,find_value,replace_value):
        if row[target_column] == find_value:
            row[target_column] = replace_value
            return row

def main(scourceId,destinationId,destinationexternalId,scourcefile,destinationfile,result):
    # File paths
    file1_path = scourcefile #'data/file1.csv'
    file2_path = destinationfile #'data/file2.csv'
    output_path = result #'data/result.csv'

    # Read CSV files into dictionaries
    file1_dict = read_csv_to_dict(file1_path, scourceId)
    file2_dict = read_csv_to_dict(file2_path, destinationexternalId)

    # Open the output file
    with open(output_path, mode='w', newline='') as output_file:
        writer = csv.writer(output_file)

        # Write the header row
        # header = list(file1_dict[next(iter(file1_dict))].keys()) + ['Lookup_Result']
        # writer.writerow(header)

        # Perform VLOOKUP for each row in file1 and write the result to the output file
        for key, row in file1_dict.items():

            lookup_result = vlookup(file2_dict, row[scourceId], destinationexternalId, destinationId)
            #output_row = list(row.values()) #+ [lookup_result]
            print(row)
            output_row = replaceColumn(list(row.values()),scourceId,row[scourceId],lookup_result)
            writer.writerow(output_row)

    print(f"VLOOKUP operation completed. Results saved to '{output_path}'.")

if __name__ == '__main__':
    main('ID','ID','ExtenalId','data/file1.csv','data/file2.csv','data/result.csv')
