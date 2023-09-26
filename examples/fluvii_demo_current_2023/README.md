Prerequisits:

- Python <=3.9
- Kafka cluster to connect to (or navigate to ./kafka and do `docker-compose up -d` to run confluents control center locally)


1. `bash setup.sh` for venv and fluvii install
2. `bash create_topics.sh`
3. `bash run_consumer.sh`
4. `bash run_producer.sh`

Watch the magic =)

`Ctrl + C` to kill apps, `docker-compose down` to kill kafka fully if using confluent 
