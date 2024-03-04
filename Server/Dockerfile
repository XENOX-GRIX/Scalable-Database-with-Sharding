# Using Ubuntu 20.04 as base image
FROM ubuntu:20.04

# # Avoiding user interaction with tzdata when installing packages
# ENV DEBIAN_FRONTEND=noninteractive

# Setting the working directory to '/dockerServer'
WORKDIR /dockerServer

# Copying current directory content to '/dockerServer'
COPY . /dockerServer

# Installing python3, pip3, flask, and MySQL 8.0
# Setting timezone which is required for docker installation
RUN apt-get update && \
    apt-get -y install sudo tzdata && \
    sudo ln -fs /usr/share/zoneinfo/Asia/Kolkata /etc/localtime && \
    sudo dpkg-reconfigure --frontend noninteractive tzdata && \
    apt-get clean

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install flask &&  pip3 install mysql-connector-python && pip3 install sqlalchemy && pip3 install flask_sqlalchemy && \
    # MySQL installation
    apt-get install -y mysql-server && \
    # Cleanup to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Setting environment variables
ENV MYSQL_DATABASE=shardsDB
ENV MYSQL_ROOT_PASSWORD=password
ENV MYSQL_USER=root

# Exposing port 5000 for the flask app
EXPOSE 5000

# # Exposing default MySQL port, optional if you plan to connect to MySQL from another container or host
# EXPOSE 3306

# Running the server
ENTRYPOINT ["python3", "server.py"]


# # Using Ubuntu 20.04 as base image
# FROM mysql:8.0-debian

# # #here the flask app deploy script is copied
# COPY deploy.sh /always-initdb.d/

# # Copying current directory content to '/server'
# COPY . /dockerServer

# # Setting the working directory to '/server'
# WORKDIR /dockerServer

# # Installing python3, pip3 and other dependencies(flask)
# RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install flask && pip3 install mysql-connector-python && pip3 install sqlalchemy && pip3 install flask_sqlalchemy

# # Exposing port 5000
# EXPOSE 5000

# # Running the server
# ENTRYPOINT python3 server.py