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

data: data/raw data/reduced data/partitioned/by/date data/partitioned/by/cve

data/raw:
	python3 scripts/epss.py --debug download --all --output-dir=data/raw --output-format=parquet

data/reduced:
	python3 scripts/epss.py --debug reduce --input-dir=data/raw --output-file=data/reduced.parquet

data/partitioned: data/partitioned/by/date data/partitioned/by/cve

data/partitioned/by/date:
	python3 scripts/epss.py --debug partition --input-file=data/reduced.parquet --output-dir=data/partitioned/by/date --by=date --output-format=parquet

data/partitioned/by/cve:
	python3 scripts/epss.py --debug partition --input-file=data/reduced.parquet --output-dir=data/partitioned/by/cve --by=cve --output-format=parquet

.PHONY: data data/raw data/reduced data/partitioned/by/date data/partitioned/by/cve
