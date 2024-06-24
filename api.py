import json
import requests
from simple_salesforce import Salesforce
import time
import csv
import datetime
from columnupdate import column_data
from vlookup2 import main

class SF:

    def __init__(self, username, password, security_token,domain):
        self.credentials = Salesforce(username=username, password=password,
                            security_token=security_token, domain=domain)#, instance_url=self.credentials.destination_instance_url)
        


    def get_lookup_fields(self, object_name):
        object_description = self.credentials.__getattr__(object_name).describe()
        reference_field_api_names = [field['name'] for field in object_description['fields'] if field['type'] == 'reference' and  field['createable']]

        print('reference_field_api_names')
        print(reference_field_api_names)

        reference_fields = {
                field['name']: field['referenceTo'][0] if field['referenceTo'] else None
                for field in object_description['fields'] if field['type'] == 'reference'
            }


        for fieldApi in reference_fields:
            breakpoint()
            print(reference_fields[fieldApi])
            self.extract_data(f'SELECT Id,Name FROM {reference_fields[fieldApi]}',f'lookup/{self.credentials.sf_instance}')
            main(fieldApi, 'ID', 'ExtenalId', 'data/{object_name}.csv', 'lookup/{self.credentials.sf_instance}', 'data/result.csv')

        

        return reference_field_api_names   
     
    def get_all_fields(self, object_name):
        object_description = self.credentials.__getattr__(object_name).describe()
        field_api_names = [field['name'] for field in object_description['fields'] if field['createable']]

        return field_api_names

    def replace_all_with_fields(self, query):
        if '[ALL]' not in query:
            return query
        
        object_name = query.split('FROM')[1].split()[0].strip()
        field_api_names = self.get_all_fields(object_name)
        fields_str = ', '.join(field_api_names)      
        modified_query = query.replace('[ALL]', fields_str)
        
        return modified_query
    
    def extract_data(self,query,folderName):
        savefolder = 'data'

        if(folderName):
            savefolder=folderName

        object_name = query.split('FROM')[1].split()[0].strip()


        query = self.replace_all_with_fields(query)

        jsonquery = json.dumps({
                "query": query,
                "operation": "query",
                "lineEnding": "CRLF",
                "contentType": "CSV"
            })
        job_response = self.credentials.restful(
            'jobs/query',
            method='POST',
            data=jsonquery
        )
        job_id = job_response['id']
        upload_url=f'jobs/query/{job_id}'
        isJobReady = False

        while not isJobReady:

            response = self.credentials.restful(
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
        upload_url=f'jobs/query/{job_id}/results'
        response = self.credentials.restful(
            upload_url,
            method='GET'
        )
        print(response)

        if response == '':
            print(f"Successfully uploaded data for job ID: {response}")
        else:
            print(f"Failed to upload data: {response}")


        with open(f'{savefolder}/{object_name}.csv', 'wb') as file:
            file.write(response)

        dt = datetime.datetime.now()
        with open(f'versionHistory/{object_name}{dt}.csv', 'wb') as file:
                    file.write(response)


        return f'{savefolder}/{object_name}.csv'
        
    # Transform data
    def transform_data(self, input_file, object_name):
        print(input_file)

        mapping = self.load_field_mapping('field_mapping.json')
        column_mapping = mapping.get(object_name, {})

        output_file = self.append_output_to_filename(input_file)
        column_data(input_file,column_mapping,output_file)

        self.get_lookup_fields(object_name)


        return output_file
    def append_output_to_filename(self, file_path):
        parts = file_path.split('.')
        parts[-2] += '__output'
        return '.'.join(parts)
    # Save data to destination org using Bulk API 2.0
    def save_data(self, object_name, file_path):

        job_response = self.credentials.restful(
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

        # Upload data
        headers = {
            'Content-Type': 'text/csv',
            'Authorization':'Bearer ' + self.credentials.session_id,
            'Accept': 'application/json'
        }
        with open(file_path, 'rb') as file:
        # Create a dictionary with the file data
            #files = {'file': file}
            print('self.credentials.session_id')
            print(file)
            print(f"{self.credentials.sf_instance}{upload_url}")
            response = requests.put(f"https://{self.credentials.sf_instance}/{upload_url}",
                                    data=file,headers=headers
                                    )
            print(requests)
            print(response)
            if response.status_code == 201:
                print(f"Successfully uploaded data for job ID: {job_id}")
            else:
                print(f"Failed to upload data: {response.content}")

        # Close job
        close_response = self.credentials.restful(
            f'jobs/ingest/{job_id}',
            method='PATCH',
            data=json.dumps({"state": "UploadComplete"})
        )

        if close_response['state'] == 'UploadComplete':
            print(f"Job {job_id} closed successfully.")
        else:
            print(f"Failed to close job: {close_response}")

        job_status, job_status_response = self.get_job_status(job_id)
        print(f"Final job status: {job_status}")

        numberRecordsFailed = job_status_response['numberRecordsFailed']
        print('numberRecordsFailed')
        print(numberRecordsFailed)
        print(numberRecordsFailed > 0)

        print(job_status_response)

        if job_status == 'Failed':
            print(f"Job failed with errors: {job_status_response['errorMessage']}")
            self.get_numberRecordsFailed(job_id,object_name)
            
        elif job_status == 'Aborted':
            print("Job was aborted.")

        if numberRecordsFailed > 0:
            self.get_numberRecordsFailed(job_id,object_name)




    def get_numberRecordsFailed(self,job_id,object_name):
            print('job_id')
            print(job_id)

            headers = {
                'Content-Type': 'text/csv',
                'Authorization':'Bearer ' + self.credentials.session_id,
                'Accept': 'application/json'
            }
            # Retrieve failed records details
            failed_results_response = self.credentials.restful(f'jobs/ingest/{job_id}/failedResults')
            print(failed_results_response)
            dt = datetime.datetime.now()
            with open(f'result/{object_name}{dt}.csv', 'wb') as file:
                file.write(failed_results_response)

    def get_job_status(self, job_id):
        while True:
            job_status_response = self.credentials.restful(f'jobs/ingest/{job_id}')
            job_state = job_status_response['state']
            if job_state in ['JobComplete', 'Failed', 'Aborted']:
                return job_state, job_status_response
            print(f"Job {job_id} status: {job_state}")
            time.sleep(10)  # Wait for 10 seconds before checking again


    def load_field_mapping(self,file_path):
        with open(file_path, 'r') as file:
            mapping = json.load(file)
        return mapping
    # Example usage