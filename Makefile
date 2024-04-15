# Checking if network exists
check_network:
	if [ -z "$$(sudo docker network ls -q -f name=my_network)" ]; then \
		echo "Network my_network does not exist. Creating..."; \
		sudo docker network create my_network; \
	else \
		echo "Network my_network already exists."; \
	fi


# Building and running the containers
run: check_network
	sudo docker build -t my-server-app ./Server
	sudo docker build -t loadbalancer ./loadBalancer
	sudo docker run -p 5000:5000 --privileged=true -v /var/run/docker.sock:/var/run/docker.sock --name my_loadbalancer_app --network my_network -it loadbalancer

# Building and running the containers using docker-compose
run_compose: check_network
	sudo docker-compose build
	sudo docker-compose up load_balancer
	
# Delete all
run_delete:
	sudo docker stop $$(sudo docker ps -a -q)
	sudo docker rm $$(sudo docker ps -a -q)
	sudo docker rmi $$(sudo docker images -q)
	sudo docker network rm my_network
	sudo docker volume rm persistentStorage
