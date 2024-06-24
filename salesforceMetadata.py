from simple_salesforce import Salesforce
import json
import credentials

# Authenticate with the source org
sf = Salesforce(username=credentials.source_username, password=credentials.source_password,
                       security_token=credentials.source_security_token, domain=credentials.source_domain)#, instance_url=credentials.source_instance_url)

# Object name (e.g., Contact, Account)
object_name = 'Contact'

# Describe the object to get all fields
object_description = sf.__getattr__(object_name).describe()

# Extract field information
print(object_description)
fields = object_description['fields']

# Print or process the field information
for field in fields:
    print(f"Field Name: {field['name']}, Field Label: {field['label']}, Field Type: {field['type']}")

field_api_names = [field['name'] for field in object_description['fields']]
createable_fields = [field['name'] for field in object_description['fields'] if field['createable']]

for field_name in createable_fields:
    print(field_name)


# Optionally save the fields to a JSON file
with open(f'{object_name}_fields.json', 'w') as json_file:
    json.dump(fields, json_file, indent=4)

print(f"All fields for {object_name} have been saved to '{object_name}_fields.json'.")
