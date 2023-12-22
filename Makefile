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

data: data/raw data/reduced.parquet data/partitioned/by/date data/partitioned/by/cve

data/raw:
	python3 scripts/epss.py download --all --output-dir=data/raw --output-format=parquet

data/reduced:
	python3 scripts/epss.py reduce --input-dir=data/raw --output-file=data/reduced.parquet

data/partitions: data/partitions/by/date data/partitions/by/cve

data/partitions/by/date:
	python3 scripts/epss.py partition --input-file=data/reduced.parquet --output-dir=data/partitions/by/date --by=date --output-format=parquet

data/partitions/by/cve:
	python3 scripts/epss.py partition --input-file=data/reduced.parquet --output-dir=data/partitions/by/cve --by=cve --output-format=parquet

.PHONY: data data/raw data/reduced data/partitions/by/date data/partitions/by/cve
