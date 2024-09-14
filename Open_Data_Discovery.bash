 
 #https://docs.opendatadiscovery.org/configuration-and-deployment/enable-security/authentication/oauth2-oidc
 # https://github.com/opendatadiscovery/odd-platform/blob/main/docker/demo.yaml
 
 #Install Docker

 --Install Docker
sudo yum install -y docker
sudo service docker start
sudo chkconfig docker on  # activates docker service auto-start on reboot
sudo usermod -a -G docker $USER # Adds current user to docker group
 
sudo adduser openDataDiscovery
sudo touch /var/log/openDataDiscovery.log
sudo chown adm:adm /var/log/openDataDiscovery.log
sudo touch /etc/default/openDataDiscovery
sudo usermod -a -G docker openDataDiscovery
sudo mkdir /data/
sudo mkdir /data/openDataDiscovery
sudo chown openDataDiscovery:openDataDiscovery /data/openDataDiscovery 
nano /etc/passwd  #change openDataDiscovery user home dir to /data/openDataDiscovery  

sudo yum install -y git
# sudo reboot

 # install docker compose for user openDataDiscovery
su - openDataDiscovery # switch to openDataDiscovery user or sudo su - openDataDiscovery
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}

#ensure that you don’t have any other PostgreSQL instance using port 5432
lsof -i -P -n | grep LISTEN | grep 5432

# If there is kill then run the lsof command again
 kill -9 <PID> 

#Clone github repo
git clone https://github.com/opendatadiscovery/odd-platform.git

# Change port to map to 8080
nano docker/demo.yaml  #change 8080 t0 8081:8080
nano docker/docker-compose.yaml 
# Use the Docker Compose to install OpenDataDiscovery utilizing the demo.yaml
docker-compose -f docker/demo.yaml up -d --build odd-platform odd-collector
#docker-compose -f docker/docker-compose.yaml up -d odd-platform-enricher odd-collector
docker-compose -f docker/docker-compose.yaml up -d --build odd-collector

docker-compose -f docker/docker-compose.yaml up -d --build



docker-compose -f demo.yaml up -d odd-collector

# clean up
docker-compose -f docker/demo.yaml down 
docker-compose -f docker/docker-compose.yaml down

# Verify the services are up and running
docker ps | grep opendatadiscovery

# If all is well, you'll get response like 
8344b643aa83   ghcr.io/opendatadiscovery/odd-platform:latest   "java -cp @/app/jib-…"   4 hours ago   Up 4 hours   0.0.0.0:8080->8080/tcp   docker-odd-platform-1


#Access the openDataDicovery Web Application
Once the services are up, you can access the openDataDicovery frontend by navigating to http://localhost:5000 in your web browser.
ENDPOINT_URL="http://localhost:8080/"
http://localhost:8081/


- delete files in sample data

#LOGS
docker-compose -f docker/docker-compose.yaml logs odd-platform
docker-compose -f docker/docker-compose.yaml logs --tail=20 odd-platform

docker-compose -f docker/docker-compose.yaml logs odd-collector
docker-compose -f docker/docker-compose.yaml logs --tail=20 odd-collector


docker-compose -f docker/docker-compose.yaml restart odd-platform
docker-compose -f docker/docker-compose.yaml restart odd-collector

docker-compose -f docker/docker-compose.yaml restart odd-platform odd-collector




nano docker/config/collector_config.yaml 


GRANT SELECT ON svv_table_info TO odd;
GRANT SELECT ON svv_columns TO odd;
GRANT SELECT ON svv_tables TO odd;

# https://docs.opendatadiscovery.org/configuration-and-deployment/trylocally#create-collector-entity
platform_host_url: http://odd-platform:8080
default_pulling_interval: 10
chunk_size: 1000
token: XY5egGFXpkU67mWdxKxdEewmWSQAdztakdeRzAnM

plugins:
  - type: redshift
    name: redshift_adapter
    description: Redshift sample database
    database: redshift_db
    host: inter-account-vpc-endpoint-endpoint-boua7ge2e1pl6l8qym41.cakrev8qfwpw.eu-west-1.redshift.amazonaws.com
    port: "5439"
    user: odd
    password: 6dU1sFiN3s6dR8b
    schemas: ['schema1', 'schema2', 'analytics_data_validation'] #If you ant all data, remove schemas
    connection_timeout: 10



#Do you actually need an enricher? This service odd-platform-enricher  was created to ingest some dummy data to the platform, 
#just to quickly ingest some data for inspecting the platform. So if you are not interested in this, 
#it is not a must-have service, only for representation purposes.



11:52
Valerii Mironchenko
the core setup is: database for odd platform, odd platform and collector

##########################################################
# Modify collector adapter.py
docker exec -it docker_odd-collector_1 /bin/bash
docker exec -u root -it docker_odd-collector_1 /bin/bash

apt install nano
nano ./odd_collector/adapters/redshift/adapter.py



from collections import defaultdict
from typing import Optional

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Generator, RedshiftGenerator

from ...domain.plugin import RedshiftPlugin
from .logger import logger
from .mappers.database import map_database
from .mappers.metadata import MetadataColumns, MetadataSchemas, MetadataTables
from .mappers.schema import map_schema
from .mappers.tables import map_tables
from .repository import RedshiftRepository


class Adapter(BaseAdapter):
    config: RedshiftPlugin
    generator: RedshiftGenerator

    def __init__(self, config: RedshiftPlugin) -> None:
        super().__init__(config)
        self.database = config.database
        self.repository = RedshiftRepository(config)

    def create_generator(self) -> Generator:
        return RedshiftGenerator(
            host_settings=self.config.host,
            databases=self.config.database,
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> Optional[DataEntityList]:
        try:
            mschemas: MetadataSchemas = self.repository.get_schemas()
            mtables: MetadataTables = self.repository.get_tables()
            mcolumns: MetadataColumns = self.repository.get_columns()
            primary_keys = self.repository.get_primary_keys()

            # Add error checking for columns
            for column in mcolumns.items:
                if not hasattr(column, 'data_type') or column.data_type is None:
                    logger.warning(f"Column {column.column_name} in table {column.table_name} has no data_type")
                    column.data_type = "UNKNOWN"  # Set a default value

            self.append_columns(mtables, mcolumns)
            self.append_primary_keys(mtables, primary_keys)

            table_entities: list[DataEntity] = []
            schema_entities: list[DataEntity] = []
            database_entities: list[DataEntity] = []

            self.generator.set_oddrn_paths(**{"databases": self.database})

            tables_by_schema = defaultdict(list)
            for mtable in mtables.items:
                tables_by_schema[mtable.schema_name].append(mtable)

            for schema in mschemas.items:
                tables = tables_by_schema.get(schema.schema_name, [])
                try:
                    table_entities_tmp = map_tables(self.generator, tables)
                    schema_entities.append(
                        map_schema(self.generator, schema, table_entities_tmp)
                    )
                    table_entities.extend(table_entities_tmp)
                except AttributeError as e:
                    logger.error(f"Error mapping tables for schema {schema.schema_name}: {e}")
                    continue  # Skip this schema if there's an error

            if not schema_entities:
                logger.error("No valid schemas found")
                return None

            database_entities.append(
                map_database(self.generator, self.database, schema_entities)
            )

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[
                    *table_entities,
                    *schema_entities,
                    *database_entities,
                ],
            )
        except Exception as e:
            logger.error(f"Failed to load metadata for tables: {e}", exc_info=True)
            return None

    @staticmethod
    def append_columns(mtables: MetadataTables, mcolumns: MetadataColumns):
        columns_by_table = defaultdict(list)
        for column in mcolumns.items:
            columns_by_table[(column.schema_name, column.table_name)].append(column)

        for table in mtables.items:
            table.columns = columns_by_table.get(
                (table.schema_name, table.table_name), []
            )

    @staticmethod
    def append_primary_keys(mtables: MetadataTables, primary_keys: list[tuple]):
        grouped_pks = defaultdict(list)
        for pk in primary_keys:
            schema_name, table_name, column_name = pk
            grouped_pks[(schema_name, table_name)].append(column_name)

        for table in mtables.items:
            table.primary_keys = grouped_pks.get(
                (table.schema_name, table.table_name), []
            )
