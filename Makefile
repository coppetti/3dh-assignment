
help:
	@echo "Usage:"
	@echo "1 - Start your db instance. At the time of the 1st run, it will download the docker image"
	@echo "make start"
	@echo ""
	@echo "2 - Impot Data"
	@echo "make import"
	@echo ""
	@echo "3 - Get Metrics"
	@echo "make metrics"
	@echo ""
	@echo "4 - Kill db"
	@echo "make kill"
	@echo ""

start:
	@sudo docker run -d -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=neo4j/admin --name="neo4j" neo4j
	@echo "Your db is being started, check when its ready accessing: http://localhost:7474. It could take a few seconds"
	@echo "user/pass: neo4j/admin"

import:
	@go run main.go import


metrics:
	@go run main.go metrics

kill:
	@sudo docker stop neo4j; docker rm neo4j