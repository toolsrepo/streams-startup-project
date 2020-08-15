package com.github.nathd.kafka.streams.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Properties;

import static com.github.nathd.kafka.streams.StreamsStarterApp.commonConfig;
import static java.util.Arrays.asList;

@Slf4j
public class ColorCountStream {

    private static final String APPLICATION_ID = "color-count-app";
    public static final String TOPIC_INPUT_COLOR = "color-input";
    public static final String TOPIC_OUTPUT_COLOR = "color-output";

    public static final List<String> validColors = asList("red", "blue", "green");

    public Properties config() {
        Properties config = commonConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        return config;
    }

    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> userAndColorTable = builder.table(TOPIC_INPUT_COLOR);

        KTable<String, Long> colorTable = userAndColorTable
                .filter((user,color) -> validColors.contains(color))
                .groupBy((user,color) -> KeyValue.pair(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColor")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
        colorTable.toStream().to(TOPIC_OUTPUT_COLOR, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
