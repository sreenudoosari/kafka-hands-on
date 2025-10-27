package com.sd.kafka.streams.basic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;

public class KafkaWithStreams {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "widget-stream-app2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Serdes.String().getClass());
        // Custom Serdes
        var stringSerde = Serdes.String();
        var widgetSerde = new WidgetSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("widgets", Consumed.with(stringSerde, widgetSerde))
                .filter((key, widget) -> "red".equalsIgnoreCase(widget.colour()))
                .mapValues(widget-> new Widget(widget.id(), widget.colour()+ "-alert", widget.size()))
                .to("widgets-red", Produced.with(stringSerde, widgetSerde));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
