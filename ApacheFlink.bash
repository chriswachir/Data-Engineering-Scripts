#!/bin/bash

docker info


# Download Apache Flink Docker image
docker pull apache/flink:1.14.2

# Run Apache Flink container in detached mode
docker run -t -d --name jobmanager -p 8081:8081 apache/flink:1.14.2 jobmanager



docker ps

# Display container information
echo "Apache Flink container is running as a daemon."
echo "Container ID: $(docker ps -q --filter "name=flink-container")"
echo "Access Flink Web Dashboard at http://localhost:8081"


telnet localhost 8081


# Example: Run a Flink job using the Flink CLI (WordCount example)
docker exec -it flink-container ./bin/flink run -m localhost:8081 ./examples/streaming/WordCount.jar

# To stop and remove the container when done
# docker stop flink-container
# docker rm flink-container

# To check container logs
# docker logs flink-container


# To run the script
./run_flink.sh

# Stop the existing Flink container
docker stop flink-container

# Remove the existing Flink container
docker rm flink-container

# Run a new Flink container with the desired name (jobmanager)
docker run -t -d --name jobmanager -p 8081:8081 apache/flink:1.14.2 jobmanager

# Get the IP address of the new Flink Job Manager
JM_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jobmanager)

echo "Flink Job Manager IP address: $JM_IP"


###NGINX
sudo yum install nginx
sudo amazon-linux-extras install nginx1

sudo systemctl start nginx
sudo systemctl enable nginx
sudo systemctl status nginx

cd /etc/nginx/htpasswd/

nano /etc/nginx/nginx.conf
or
nano /etc/nginx/sites-available/default

server {
    listen 80;
    server_name flink.yourdomain.com;

    location / {
        proxy_pass http://localhost:8081;  # Assuming Flink dashboard is running on port 8081
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Add authentication configurations here
        # Example: Basic HTTP authentication
        auth_basic "Restricted Access";
        auth_basic_user_file /etc/nginx/.htpasswd;
    }
}

sudo service nginx restart
or 
sudo systemctl reload nginx

or 
sudo systemctl start nginx
sudo systemctl stop nginx
sudo systemctl status nginx


sudo tail -f /var/log/nginx/error.log




# Check for syntax errors
sudo nginx -t

# Checking for logs
sudo cat /var/log/nginx/error.log


# Create an .htpasswd file to store usernames and hashed passwords. Replace <username> with your desired username.
sudo sh -c "echo -n 'odd_user:' >> /etc/nginx/.htpasswd"
sudo sh -c "openssl passwd -aug1 >> /etc/nginx/.htpasswd"

or
sudo mkdir -p /etc/nginx/htpasswd/
sudo htpasswd -c -b /etc/nginx/htpasswd/htpasswd-users flink_user your_password
htpasswd -c -p -b /etc/nginx/htpasswd/htpasswd-users flink_user your_plain_password

sudo htpasswd -c -b /etc/nginx/htpasswd/htpasswd-users CellulantDE KRybAqiFujERSRTr

#Add another user without overriting current user
sudo htpasswd -b /etc/nginx/htpasswd/htpasswd-users another_user another_password


cd /etc/nginx/htpasswd/
cat htpasswd-users




####configuration
#sudo systemctl reload nginx
# For more information on configuration, see:
#   * Official English Documentation: http://nginx.org/en/docs/
#   * Official Russian Documentation: http://nginx.org/ru/docs/

user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log;
pid /run/nginx.pid;

# Load dynamic modules. See /usr/share/doc/nginx/README.dynamic.
include /usr/share/nginx/modules/*.conf;

events {
    worker_connections 1024;
}

http {
    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';

    access_log  /var/log/nginx/access.log  main;

    sendfile            on;
    tcp_nopush          on;
    tcp_nodelay         on;
    keepalive_timeout   65;
    types_hash_max_size 4096;

    include             /etc/nginx/mime.types;
    default_type        application/octet-stream;

    # Load modular configuration files from the /etc/nginx/conf.d directory.
    # See http://nginx.org/en/docs/ngx_core_module.html#include
    # for more information.
    include /etc/nginx/conf.d/*.conf;
    server {
        listen       80;
        listen       [::]:80;
        server_name  flink.cellulant.africa;
        root         /usr/share/nginx/html;

        # Load configuration files for the default server block.
        include /etc/nginx/default.d/*.conf;

        location / {
            proxy_pass http://localhost:8081;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Add authentication configuration here
            # Example: Basic HTTP authentication
            auth_basic "Restricted Access";
            auth_basic_user_file /etc/nginx/htpasswd/htpasswd-users;
        }

        error_page 404 /404.html;
        location = /404.html {
        }

        error_page 500 502 503 504 /50x.html;
        location = /50x.html {
        }
    }


# Settings for a TLS enabled server.
# Settings for a TLS enabled server.
#
#    server {
#        listen       443 ssl http2;
#        listen       [::]:443 ssl http2;
#        server_name  _;
#        root         /usr/share/nginx/html;
#
#        ssl_certificate "/etc/pki/nginx/server.crt";
#        ssl_certificate_key "/etc/pki/nginx/private/server.key";
#        ssl_session_cache shared:SSL:1m;
#        ssl_session_timeout  10m;
#        ssl_ciphers PROFILE=SYSTEM;
#        ssl_prefer_server_ciphers on;
#
#        # Load configuration files for the default server block.
#        include /etc/nginx/default.d/*.conf;
#
#        error_page 404 /404.html;
#            location = /40x.html {
#        }
#
#        error_page 500 502 503 504 /50x.html;
#            location = /50x.html {
#        }
#    }

}
