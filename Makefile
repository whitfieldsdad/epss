default: clean install

clean:
	rm -rf data

update:
	poetry update

requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes
