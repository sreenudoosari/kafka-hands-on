#PRODUCER COMMANDS:

#Send message via CLI
docker exec -it kafka-1 bash
/usr/bin/kafka-topics --create --topic test-topic --bootstrap-server kafka-1:9092
/usr/bin/kafka-console-producer --topic test-topic --bootstrap-server kafka-1:9092

#Produce with key and value:
/usr/bin/kafka-console-producer  --topic test-topic --bootstrap-server kafka-1:9092 --property parse.key=true --property key.separator=:

#CONSUMER COMMANDS:
/usr/bin/kafka-console-consumer --bootstrap-server kafka-1:9092  --topic test-topic --from-beginning

#Consumer with key and value:
/usr/bin/kafka-console-consumer --bootstrap-server kafka-1:9092  --topic test-topic --from-beginning  --property print.key=true --property key.separator=:

#TOPIC commands:
/usr/bin/kafka-topics --list --bootstrap-server kafka-1:9092
/usr/bin/kafka-topics --describe --topic test-topic --bootstrap-server kafka-1:9092

#Modify/Alter commands:
/usr/bin/kafka-topics --alter --bootstrap-server kafka-1:9092 --topic test-topic --partitions 2
/usr/bin/kafka-configs --bootstrap-server kafka-1:9092 --entity-type topics --entity-name test-topic --alter --add-config retention.ms=86400000

# To know the config parameter to alter
/usr/bin/kafka-configs --help
# To list all the dynamically ALTERED parameters of a particular topic
/usr/bin/kafka-configs --bootstrap-server kafka-1:9092 --entity-type topics --entity-name test-topic --describe

/usr/bin/kafka-topics --delete --topic test-topic --bootstrap-server kafka-1:9092

#With SSL:
/usr/bin/kafka-topics  --list --bootstrap-server kafka-1:9092 --command-config /etc/kafka/client-ssl.properties


| Property              | Description                       | Default       |
| --------------------- | --------------------------------- | ------------- |
| `retention.ms`        | How long messages are retained    | 7 days        |
| `retention.bytes`     | Max log size before deletion      | -1 (no limit) |
| `cleanup.policy`      | `delete` or `compact`             | delete        |
| `min.insync.replicas` | Required acks for produce success | 1             |
| `max.message.bytes`   | Max message size per topic        | 1 MB          |

Field	Meaning:
ISR	            In-Sync Replicas (currently up to date)
ELR	            Eligible Leader Replicas (brokers that can take leadership)
LastKnownElr	  ELR recorded in the last metadata update



#BROKER INFORMATION
/usr/bin/kafka-cluster list-endpoints --bootstrap-server kafka-0:9092

/usr/bin/kafka-broker-api-versions --bootstrap-server kafka-1:9092

#Check with UI tools



docker volume create kafka_0_data
docker volume create kafka_1_data
docker volume create kafka_2_data