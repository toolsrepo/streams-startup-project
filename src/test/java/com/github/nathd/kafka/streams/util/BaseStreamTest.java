package com.github.nathd.kafka.streams.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;

import java.util.Properties;

public abstract class BaseStreamTest {

    protected final StringSerializer stringSerializer = new StringSerializer();
    protected TopologyTestDriver testDriver;
    protected TestInputTopic<String, String> inputTopic;
    protected TestOutputTopic<String, Long> outputTopic;

    @After
    public void cleanup() {
        testDriver.close();
    }

    public Properties commonConfig(String applicationId, String bootstrapServers) {
        java.util.Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return config;
    }

    public void pushNewRecord(String key, String value) {
        inputTopic.pipeInput(key, value);
    }

    public TestRecord<String, Long> readOutput() {
        return outputTopic.readRecord();
    }
}
