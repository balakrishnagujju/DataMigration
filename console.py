
import credentials
from api import SF

# Authenticate with the source org
sf_source = SF(username=credentials.source_username, password=credentials.source_password,
                       security_token=credentials.source_security_token, domain=credentials.source_domain)#, instance_url=credentials.source_instance_url)

# Authenticate with the destination org
sf_destination = SF(username=credentials.destination_username, password=credentials.destination_password,
                            security_token=credentials.destination_security_token, domain=credentials.destination_domain)#, instance_url=credentials.destination_instance_url)




query = "SELECT Site,Website,Name FROM Account"
source_data = sf_source.extract_data(query,False)
print('====================')
transformed_data = sf_destination.transform_data(source_data,'Account')
print(transformed_data)
print('========!!!!!!!!!!!============')

sf_destination.save_data('Account', transformed_data)
