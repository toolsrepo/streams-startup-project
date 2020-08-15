package com.github.nathd.kafka.streams;

import com.github.nathd.kafka.streams.example.ColorCountStream;
import com.github.nathd.kafka.streams.example.WordCountStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class StreamsStarterApp {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static Properties commonConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return config;
    }

    public static void main(String[] args) {
        List<KafkaStreams> streamsList = new LinkedList<>();
        WordCountStream wordCountStream = new WordCountStream();
        ColorCountStream colorCountStream = new ColorCountStream();

//        streamsList.add(new KafkaStreams(wordCountStream.topology(), wordCountStream.config()));
        streamsList.add(new KafkaStreams(colorCountStream.topology(), colorCountStream.config()));

        streamsList.forEach(streams -> {
            // only do this in dev - not in prod
            streams.cleanUp();
            streams.start();

            // print the topology
            streams.localThreadsMetadata().forEach(data -> log.info(data.toString()));

            // shutdown hook to correctly close the kafkaStreamsWordCount application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } );

    }

}
