package com.sd.kafka.streams.statestore.interactive;


import com.sd.kafka.avro.ApplianceOrder;
import com.sd.kafka.avro.CombinedOrder;
import com.sd.kafka.avro.ElectronicOrder;
import com.sd.kafka.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsJoin {

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        streamsProps.put("schema.registry.url", "http://localhost:8085");
        streamsProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        streamsProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7000");
        //if you want to control the path of the rocks db stored in the host of your kafka streams app
        //streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, "/my/custom/dir");
        StreamsBuilder builder = new StreamsBuilder();

        String streamOneInput = "stream_interactive_one.input.topic";
        String streamTwoInput = "stream_interactive_two.input.topic";
        String tableInput = "stream_interactive_table.input.topic";
        String outputTopic ="stream_interactive_joins.output.topic";

        Map<String, Object> configMap =  propertiesToMap(streamsProps);

        SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                        .setApplianceOrderId(applianceOrder.getOrderId())
                        .setApplianceId(applianceOrder.getApplianceId())
                        .setElectronicOrderId(electronicOrder.getOrderId())
                        .setTime(Instant.now().toEpochMilli())
                        .build();

        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };

        KStream<String, ApplianceOrder> applianceStream =
                builder.stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde))
                        .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        KStream<String, ElectronicOrder> electronicStream =
                builder.stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

        KTable<String, User> userTable =
                builder.table(tableInput, Materialized.with(Serdes.String(), userSerde));

        KStream<String, CombinedOrder> combinedStream =
                applianceStream.join(
                                electronicStream,
                                orderJoiner,
                                JoinWindows.of(Duration.ofMinutes(30)),
                                StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde))
                        .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));

        combinedStream.leftJoin(
                        userTable,
                        enrichmentJoiner,
                        Joined.with(Serdes.String(), combinedSerde, userSerde))
                .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                //.to(outputTopic, Produced.with(Serdes.String(), combinedSerde))
                // Add this: materialize as state store
                .toTable(Materialized.<String, CombinedOrder, KeyValueStore<Bytes, byte[]>>as("combined-orders-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(combinedSerde));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();

                // wait until kafka streams is RUNNING
                while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                    Thread.sleep(100);
                }

                // wait until the store is queryable
                ReadOnlyKeyValueStore<String, CombinedOrder> store = null;
                while (store == null) {
                    try {
                        store = kafkaStreams.store(
                                StoreQueryParameters.fromNameAndType("combined-orders-store", QueryableStoreTypes.keyValueStore())
                        );
                    } catch (Exception e) {
                        Thread.sleep(500); // retry until store is available
                    }
                }

                Thread.sleep(5000); // give streams time to process produced records

                CombinedOrder order = store.get("10261999");
                System.out.println("Query result for userId=10261998 => " + order);

                // keep app alive so it doesn’t exit immediately
                shutdownLatch.await();

            } catch (Throwable e) {
                System.out.printf("error occurred :"+e);
                System.exit(1);
            }



        System.exit(0);
        }
    }

    private static Map<String, Object> propertiesToMap(Properties props) {
        Map<String, Object> map = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            map.put(name, props.get(name));
        }
        return map;
    }

           /*
        Rule of thumb:
            Materialized.as("name") → explicitly persistent, queryable RocksDB store.
            Materialized.with(...) without .as() → used internally, may still use RocksDB, but usually not queryable by name.

            Inspect rocksdb:
             cd  /tmp/kafka-streams/joining-streams/0_0/rocksdb/combined-orders-store

             . Summarized workflow

                Input records consumed.

                Streams executes your joins.

                Output CombinedOrder stored in RocksDB (combined-orders-store).

                RocksDB entry replicated to ...-changelog topic. ( here it is : joining-streams-combined-orders-store-changelog )

                Query API (store.get(...)) reads directly from RocksDB.

                Install RocksDB CLI (apt-get install -y rocksdb-tools on Debian/Ubuntu).

                ldb --db=/tmp/kafka-streams/joining-streams/0_0/rocksdb/combined-orders-store/ list

                By default, Kafka Streams uses RocksDB for local state because:

                        It’s embeddable (runs in-process, no network calls).

                        It’s persistent (survives restarts via changelog topics).

                        It can handle large state on disk (not limited by memory).

                        But yes — you can replace RocksDB with another store implementation, even Redis.

                        You can plug in your own store backend by implementing:

                                StateStore (or extend AbstractStateStore)

                                KeyValueStore<K, V>

                                A corresponding StoreBuilder


                                2. Trade-offs if using Redis

                    Performance: RocksDB is in-process, Redis requires network calls, so it will be slower.

                    Durability: Kafka Streams already uses changelog topics for durability. RocksDB integrates nicely; with Redis you’d have two persistence layers (Kafka changelog + Redis persistence).

                    Scaling: Redis can scale horizontally (cluster mode). RocksDB is local to one instance.

                    Querying: With Redis, you could even query state externally (not only inside Kafka Streams).

    Conclusion:
            You can replace RocksDB with Redis, but you’d have to implement a custom StateStore. In practice, most people either:

            Stick with RocksDB (best integration, persistent, fast enough).

            Use inMemoryKeyValueStore for lightweight ephemeral state.

            Push state out to external DBs (Redis, Cassandra, etc.) via sinks, but not as the primary store for joins/aggregations.

         */
}