package com.github.nathd.kafka.streams.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static com.github.nathd.kafka.streams.StreamsStarterApp.commonConfig;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.kstream.Named.as;

@Slf4j
public class WordCountStream {

    private static final String APPLICATION_ID = "word-count-app";
    public static final String TOPIC_INPUT_WC = "word-count-input";
    public static final String TOPIC_OUTPUT_WC = "word-count-output";

    public Properties config() {
        Properties config = commonConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        return config;
    }

    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream(TOPIC_INPUT_WC);

        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase(), as("convert_to_lower_case"))
                .flatMapValues(lower -> asList(lower.split("\\s+")), as("word_list_from_a_sentence"))
                .selectKey((key, word) -> word, as("select_each_word_as_new_key"))
                .groupByKey()
                .count(as("Counts"));

        wordCounts.toStream().to(TOPIC_OUTPUT_WC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
