package com.github.nathd.kafka.streams.example;

import com.github.nathd.kafka.streams.util.BaseStreamTest;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.github.nathd.kafka.streams.example.ColorCountStream.TOPIC_INPUT_COLOR;
import static com.github.nathd.kafka.streams.example.ColorCountStream.TOPIC_OUTPUT_COLOR;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class ColorCountStreamTest extends BaseStreamTest {

    @Before
    public void setup() {
        Properties config = commonConfig("color-test", "dummy:1234");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        testDriver = new TopologyTestDriver(new ColorCountStream().topology(), config);
        inputTopic = testDriver.createInputTopic(TOPIC_INPUT_COLOR, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(TOPIC_OUTPUT_COLOR, new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void testColors() {
        pushNewRecord("foo", "red");
        pushNewRecord("bar", "green");
        pushNewRecord("foo", "blue");
        pushNewRecord("mark", "red");
        pushNewRecord("alex", "blue");

        String[] attributes = new String[] {"key", "value"};
        assertThat(readOutput()).extracting(attributes).contains("red", 1L);
        assertThat(readOutput()).extracting(attributes).contains("green", 1L);
        assertThat(readOutput()).extracting(attributes).contains("red", 0L);
        assertThat(readOutput()).extracting(attributes).contains("blue", 1L);
        assertThat(readOutput()).extracting(attributes).contains("red", 1L);
        assertThat(readOutput()).extracting(attributes).contains("blue", 2L);

    }

}
