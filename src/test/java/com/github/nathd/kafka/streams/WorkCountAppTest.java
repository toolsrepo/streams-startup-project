package com.github.nathd.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class WorkCountAppTest {

    private final StringSerializer stringSerializer = new StringSerializer();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setup() {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(new StreamsStarterApp().createTopology(), config);
        inputTopic = testDriver.createInputTopic("word-count-input", stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void cleanup() {
        testDriver.close();
    }

    public void pushNewRecord(String value) {
        inputTopic.pipeInput(value);
    }

    public TestRecord<String, Long> readOutput() {
        return outputTopic.readRecord();
    }

    @Test
    public void testLowerCase() {
        String firstRecord = "Kafka KAFKA kafka";
        pushNewRecord(firstRecord);
        TestRecord<String, Long> read1 = readOutput();
        TestRecord<String, Long> read2 = readOutput();
        TestRecord<String, Long> read3 = readOutput();
        assertThat(read1.key()).isEqualTo("kafka");
        assertThat(read2.key()).isEqualTo("kafka");
        assertThat(read3.key()).isEqualTo("kafka");

        assertThat(read3.value()).isEqualTo(3);

        String secondRecord ="Testing Kafka again";
        pushNewRecord(secondRecord);
        TestRecord<String, Long> read4 = readOutput();
        TestRecord<String, Long> read5 = readOutput();
        TestRecord<String, Long> read6 = readOutput();

        assertThat(read4.key()).isEqualTo("testing");
        assertThat(read5.key()).isEqualTo("kafka");
        assertThat(read6.key()).isEqualTo("again");

        assertThat(read5.value()).isEqualTo(4);
    }

}
