# https://www.amundsen.io/amundsen/developer_guide/
 
 #Install Docker

 --Install Docker
sudo yum install -y docker
sudo service docker start
sudo chkconfig docker on  # activates docker service auto-start on reboot
sudo usermod -a -G docker $USER # Adds current user to docker group
 
sudo adduser amundsen
sudo touch /var/log/amundsen.log
sudo chown adm:adm /var/log/amundsen.log
sudo touch /etc/default/amundsen
sudo usermod -a -G docker amundsen
sudo mkdir /data/
sudo mkdir /data/amundsen
sudo chown amundsen:amundsen /data/amundsen 
nano /etc/passwd  #change amundsen user home dir to /data/amundsen  

sudo yum install -y git
# sudo reboot

 # install docker compose for user amundsen
su - amundsen # switch to amundsen user or sudo su - amundsen
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}

#Clone the Amundsen Repository
git clone https://github.com/amundsen-io/amundsen.git
cd amundsen

# If you have already cloned the repository but your submodules are empty, from your cloned amundsen directory run:
git submodule init
git submodule update

# Ensure you have the latest code¶
git submodule update --remote

#Navigate to the Docker Directory
cd amundsen/docker-amundsen

# Set Up the Environment Variables

cp env.example .env

vim .env

# Launch Amundsen Using Docker Compose
docker-compose -f docker-amundsen.yml up -d

#After making local changes rebuild and relaunch modified containers:

docker-compose -f docker-amundsen-atlas.yml build \ && docker-compose -f docker-amundsen-local.yml up -d


#Access the Amundsen Web Application
Once the services are up, you can access the Amundsen frontend by navigating to http://localhost:5000 in your web browser.
ENDPOINT_URL="http://localhost:5000/"


# Check the Status of Services
docker-compose ps

# Stopping the Services
docker-compose down
docker-compose -f docker-amundsen.yml down


## Developing the Dockerfile
#Use the docker build command to build the image. 
docker build -t amundsen-frontend:local -f Dockerfile.frontend.local .

# RUn the container
docker run -it --name=debug amundsen-frontend:local /bin/sh



