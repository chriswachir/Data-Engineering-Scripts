 https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
 
 #Install Docker

 --Install Docker
sudo yum install -y docker
sudo service docker start
sudo chkconfig docker on  # activates docker service auto-start on reboot
sudo usermod -a -G docker $USER # Adds current user to docker group
 
sudo adduser airflow
sudo touch /var/log/airflow.log
sudo chown adm:adm /var/log/airflow.log
sudo touch /etc/default/airflow
sudo usermod -a -G docker airflow
sudo mkdir /data/
sudo mkdir /data/airflow
sudo chown airflow:airflow /data/airflow 
nano /etc/passwd  #change airflow user home dir to /data/airflow  

sudo yum install -y git
# sudo reboot

 # install docker compose for user airflow
su - airflow # switch to airflow user or sudo su - airflow
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}

mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

docker compose version

 # Running Airflow in Docker
 # https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config ./inputs ./outputs
echo -e "AIRFLOW_UID=$(id -u)" > .env

AIRFLOW_UID=50000

docker compose up airflow-init

docker compose up

docker compose up -d # run deamon

# In a second terminal you can check the condition of the containers and make sure that no containers are in an unhealthy condition:
docker ps

      
# Accessing the environment
docker compose run airflow-worker airflow info

#make your work easier and download a optional wrapper scripts that will allow you to run commands with a simpler command.
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/airflow.sh'
chmod +x airflow.sh

./airflow.sh info


# Sending requests to the REST API
ENDPOINT_URL="http://localhost:8080/"
curl -X GET  \
    --user "airflow:airflow" \
    "${ENDPOINT_URL}/api/v1/pools"

# Cleaning up
docker compose down --volumes --rmi all


################################################################################################
# Dags
docker ps
docker exec -it airflow-airflow-webserver-1 /bin/bash
docker exec -u root -it airflow-airflow-webserver-1 /bin/bash

cd /opt/airflow/dags
cd /data/airflow/dags


ls -al

su - airflow
# Loading dags
 su - airflow
-bash-4.2$ cd /opt/airflow/dags
-bash-4.2$ ls
airflow  airflow.sh  config  dags  docker-compose.yaml  logs  plugins
-bash-4.2$ cd dags/
-bash-4.2$ ls


nano ~/airflow/airflow.cfg

nano airflow.cfg

docker restart airflow-airflow-webserver-1
docker restart airflow-airflow-scheduler-1

airflow scheduler
airflow webserver

## Installing pymysql(libraries)
docker exec -it 1c0acd1139d4 /bin/bash
pip install pymysql
python -c "import pymysql; print(pymysql.__version__)"
exit




docker compose down
docker compose up

nano my_first_dag.py
sudo yum update
sudo yum install nano


find / | grep dags

airflow dags list

airflow config get-value core load_examples

find / -type f -name "docker-compose.yaml"
nano /data/airflow/docker-compose.yaml


airflow send_email --subject "Test Email" --to "christopher.wachira@cellulant.io" --html "This is a test email."

python3 -m venv myenv
source myenv/bin/activate
python3
https://www.youtube.com/watch?v=urVT5BE3XCM


# changing sqllite3
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >> ~/.bashrc
source ~/.bashrc

export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >> ~/.bash_profile
source ~/.bash_profile


airflow dags trigger <DAG_ID>

airflow dags delete dataset_consumes_1



echo $AIRFLOW__CORE__LOAD_EXAMPLES
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Checking user
ps -ef | grep airflow
