# Using mysql:8.0.36-debian as base image
FROM mysql:8.0.36-debian

# # Avoiding user interaction with tzdata when installing packages
# ENV DEBIAN_FRONTEND=noninteractive

# Copying current directory content to '/dockerServer'
COPY . /docker-entrypoint-initdb.d/

# Installing python3, pip3, flask, flask-sqlalchemy and mysql-connector-python
RUN apt-get update 
RUN apt-get install -y python3-full
RUN apt-get install -y python3-pip
RUN pip install --break-system-packages --upgrade pip
RUN pip install --break-system-packages -r /docker-entrypoint-initdb.d/requirements.txt

# Setting environment variables
ENV MYSQL_DATABASE="shardsDB"
ENV MYSQL_ROOT_PASSWORD="password"
ENV MYSQL_USER="server"
ENV MYSQL_PASSWORD="password"

# Exposing port 5000 for the flask app
EXPOSE 5000