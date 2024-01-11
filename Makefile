default: clean install

clean:
	rm -rf data

update:
	poetry update

requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes

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
