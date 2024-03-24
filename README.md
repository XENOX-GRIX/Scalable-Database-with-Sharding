# Scalable-Database-with-Sharding

## To Run server
### If logs not required to build
- sudo docker build -t my-server-app . 
### If logs required to build
- sudo docker build --no-cache --progress=plain -t my-server-app .
### To Run
- sudo docker run -p 5000:5000 -e "SERVER_ID=123456" -e "MYSQL_USER=server" -e "MYSQL_PASSWORD=abc" -e "MYSQL_DATABASE=shardsDB" -e "MYSQL_HOST=localhost" -e "MYSQL_PORT=3306" --name server1 my-server-app


To run the load-balancer :
```
make run 
```