import requests
import yaml
from databricks.sdk.runtime import sql

class SolAccManager:
  def __init__(self, config_file_path='./config.yml'):
        with open(config_file_path, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
        self.config= config
        self.catalog_name = config['catalog_name']

  def create_catalog(self):
    if sql(f"SHOW CATALOGS LIKE '{self.catalog_name}'").count()==0:
      sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")

      # Grant create and use catalog permissions for the catalog to all users on the account.
      # This also works for other account-level groups and individual users.
      sql(f"""GRANT CREATE, USE CATALOG ON CATALOG {self.catalog_name} TO `account users`""")
      print(f'catalog name:{self.catalog_name} is created and access to account users is granted.')

    else:
      print(f'catalog name:{self.catalog_name} already exists')
  
  def add_schema(self,schema_name):
    # Create a schema in the catalog that was set earlier.
    if sql(f"SHOW SCHEMAS IN `{self.catalog_name}` LIKE '{schema_name}'").count()==0:
      sql(f"""CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{schema_name}""")
    else:
      print(f'schema:{schema_name} already exists')

  @staticmethod
  def download_file(url, destination):
    # Send a GET request to the URL to fetch the data
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the destination file in binary write mode
        with open(destination, 'wb') as f:
            # Write the content of the response to the file
            f.write(response.content)
        print(f"File downloaded successfully and is available at {destination}!")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")