import pandas as pd

import json


def column_data(input_file, column_mapping, outputfile):

    df = pd.read_csv(input_file)


    # Rename the columns
    df.rename(columns=column_mapping, inplace=True)

    # Print the new column names
    print("New column names:", df.columns)

    # Save the modified DataFrame to a new CSV file
    df.to_csv(outputfile, index=False, lineterminator='\r\n')


# with open('field_mapping.json', 'r') as file:
#     mapping = json.load(file)
#     column_mapping = mapping.get('Account', {})
#     transform_data('data/Account.csv',column_mapping)