package com.sd.kafka.streams.join;


import com.sd.kafka.avro.ApplianceOrder;
import com.sd.kafka.avro.ElectronicOrder;
import com.sd.kafka.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() throws IOException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_topic_ktable");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put("schema.registry.url", "http://localhost:8085");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Callback callback = (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };

        try(Admin adminClient = Admin.create(properties);
            Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties)) {
            String leftSideTopic = "stream_one.input.topic";
            String rightSideTopic = "stream_two.input.topic";
            String tableTopic = "stream_table.input.topic";
            String outputTopic ="stream_joins.output.topic";
            var topics = List.of( new NewTopic(leftSideTopic, 1, (short) 1),
                    new NewTopic(rightSideTopic, 1, (short) 1),
                    new NewTopic(tableTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1));
            adminClient.createTopics(topics);

            ApplianceOrder applianceOrderOne = ApplianceOrder.newBuilder()
                                                             .setApplianceId("dishwasher-1333")
                                                             .setOrderId("remodel-1")
                                                             .setUserId("10261998")
                                                             .setTime(Instant.now().toEpochMilli()).build();

            ApplianceOrder applianceOrderTwo = ApplianceOrder.newBuilder()
                                                             .setApplianceId("stove-2333")
                                                             .setOrderId("remodel-2")
                                                             .setUserId("10261999")
                                                             .setTime(Instant.now().toEpochMilli()).build();
            var applianceOrders = List.of(applianceOrderOne, applianceOrderTwo);

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("television-2333")
                    .setOrderId("remodel-1")
                    .setUserId("10261998")
                    .setTime(Instant.now().toEpochMilli()).build();

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("laptop-5333")
                    .setOrderId("remodel-2")
                    .setUserId("10261999")
                    .setTime(Instant.now().toEpochMilli()).build();

            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo);


            User userOne = User.newBuilder().setUserId("10261998").setAddress("5405 6th Avenue").setName("Elizabeth Jones")
                    .setCountry("FR").setPhone("123").build();
            User userTwo = User.newBuilder().setUserId("10261999").setAddress("407 64th Street").setName("Art Vandelay")
                    .setCountry("US").setPhone("456").build();

            var users = List.of(userOne, userTwo);

            // Users must be created first so the KTable records have earlier timestamps than the KStream records,
            // otherwise the join will ignore them.
            users.forEach(user -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<String, SpecificRecord>(tableTopic, user.getUserId().toString(), user);
                producer.send(producerRecord, callback);
            });

            applianceOrders.forEach((ao -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<String, SpecificRecord>(leftSideTopic, ao.getUserId().toString(), ao);
                producer.send(producerRecord, callback);
            }));

            electronicOrders.forEach((eo -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<String, SpecificRecord>(rightSideTopic, eo.getUserId().toString(), eo);
                producer.send(producerRecord, callback);
            }));
        }
    }
}
