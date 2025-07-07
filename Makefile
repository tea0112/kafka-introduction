.PHONY: producer consumer kafka
consumer:
	go run consumer/main.go
producer:
	go run producer/main.go
kafka:
	docker compose -f ./kafka-container/docker-compose.yml up -d
create-topic:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic user-activity --partitions 3 --replication-factor 1
describe:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic user-activity