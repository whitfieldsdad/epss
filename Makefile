default: clean install

clean:
	mvn clean

install:
	mvn install

up: container
	docker-compose up --force-recreate

container:
	docker-compose build --no-cache

down:
	docker-compose down --remove-orphans

destroy:
	docker-compose down --remove-orphans --rmi all --volumes

nuke: destroy

restart: down up
