import json
import requests
from simple_salesforce import Salesforce
#import async_callback
import time
import csv


# Salesforce source org credentials

source_username = 'balakrishna.gujju@icloud.com.ar'
source_password = 'balu@123'
source_security_token = 'eIzNa4N5CzTiy1nZlaj16St3'
source_instance_url = 'http://archiving-dev-ed.develop.my.salesforce.com'
source_domain = 'login'  # Use 'test' for sandbox


# Salesforce destination org credentials
destination_username = 'balakrishna.gujju@icloud.com.bo'
destination_password = 'balu@123'
destination_security_token = 'DhfWKiUFnpZHFJFd1FqYvuVKZ'
destination_domain = 'login'  # Use 'test' for sandbox
destination_instance_url = 'https://healthcarebo-dev-ed.develop.my.salesforce.com'

# Authenticate with the source org
sf_source = Salesforce(username=source_username, password=source_password,
                       security_token=source_security_token, domain=source_domain)#, instance_url=source_instance_url)

# Authenticate with the destination org
sf_destination = Salesforce(username=destination_username, password=destination_password,
                            security_token=destination_security_token, domain=destination_domain)#, instance_url=destination_instance_url)

# Extract data from source org
def extract_data(query):
    jsonquery = json.dumps({
            "query": query,
            "operation": "query",
            "lineEnding": "CRLF",
            "contentType": "CSV"
        })
    print(jsonquery)
    job_response = sf_destination.restful(
        'jobs/query',
        method='POST',
        data=jsonquery
    )

    job_id = job_response['id']
    print(job_response)
    
    #status
    
    headers = {
        'Content-Type': 'application/json'
    }
    upload_url=f'jobs/query/{job_id}'
    print(job_id)

    isJobReady = False

    while not isJobReady:

        response = sf_destination.restful(
        upload_url,
        method='GET'
        )
        if response['state'] == 'JobComplete':
            print(f"Successfully uploaded data for job ID: {job_id}")
            isJobReady = True
        else:
            print(f"Failed to upload data: {response}")
            #raise Exception("This is a general exception")

        time.sleep(5) 

    print('End')

    # fetch
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'test/csv'
    }

    upload_url=f'jobs/query/{job_id}/results'
    response = sf_destination.restful(
        upload_url,
        method='GET'
    )
    print(response)

    if response == '':
        print(f"Successfully uploaded data for job ID: {response}")
    else:
        print(f"Failed to upload data: {response}")
    print('start response')
    print(response)
    print('end response')


    with open('downloaded_file.csv', 'wb') as file:
        file.write(response)
    return 'downloaded_file.csv'
    

#waiting thread
#async_callback(5, onprocesscomplete , handle_async_result)

print("Async operation started, waiting for result...")

# Transform data
def transform_data(input_file):
    print(input_file)

    with open(input_file, mode='r', newline='') as file:
            csv_reader = csv.DictReader(file)
            # Get the fieldnames from the original file
            fieldnames = csv_reader.fieldnames
            print(fieldnames)
    return input_file
    # transformed_records = []
    # for record in records:
    #     # Example transformation: Add a new field with a default value
    #     record['NewField__c'] = 'DefaultValue'
    #     transformed_records.append(record)
    # return transformed_records

# Save data to destination org using Bulk API 2.0
def save_data_to_destination(object_name, file_path):




    job_response = sf_destination.restful(
        'jobs/ingest',
        method='POST',
        data=json.dumps({
            "object": object_name,
            "operation": "insert",
            "lineEnding": "CRLF",
            "contentType": "CSV"
        })
    )

    job_id = job_response['id']
    upload_url = job_response['contentUrl']
    breakpoint()

    # Upload data
    headers = {
        'Content-Type': 'text/csv',
        'Authorization':'Bearer ' + sf_destination.session_id,
    }
    with open(file_path, 'rb') as file:
    # Create a dictionary with the file data
        #files = {'file': file}
        print('sf_destination.session_id')
        print(file)
        print(f"{destination_instance_url}{upload_url}")
        response = requests.put(f"{destination_instance_url}/{upload_url}",
                                files = {"form_field_name": file},headers=headers
                                )
        print(requests)
        print(response)
        if response.status_code == 201:
            print(f"Successfully uploaded data for job ID: {job_id}")
        else:
            print(f"Failed to upload data: {response.content}")
    breakpoint()

    # Close job
    close_response = sf_destination.restful(
        f'jobs/ingest/{job_id}',
        method='PATCH',
        data=json.dumps({"state": "UploadComplete"})
    )

    if close_response['state'] == 'UploadComplete':
        print(f"Job {job_id} closed successfully.")
    else:
        print(f"Failed to close job: {close_response}")

# Example usage
query = "SELECT Name FROM Account"
source_data = extract_data(query)
transformed_data = transform_data(source_data)
save_data_to_destination('Account', transformed_data)
