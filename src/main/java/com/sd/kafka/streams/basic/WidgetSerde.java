package com.sd.kafka.streams.basic;

import org.apache.kafka.common.serialization.Serdes;

public class WidgetSerde extends Serdes.WrapperSerde<Widget> {

    public WidgetSerde() {
        super(new WidgetJsonSerializer(), new WidgetJsonDeserializer());
    }
}
