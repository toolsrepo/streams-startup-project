package com.github.nathd.kafka.streams.example;

import com.github.nathd.kafka.streams.util.BaseStreamTest;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Before;
import org.junit.Test;

import static com.github.nathd.kafka.streams.example.WordCountStream.TOPIC_INPUT_WC;
import static com.github.nathd.kafka.streams.example.WordCountStream.TOPIC_OUTPUT_WC;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class WordCountStreamTest extends BaseStreamTest {

    @Before
    public void setup() {
        testDriver = new TopologyTestDriver(new WordCountStream().topology(),
                commonConfig("word-test", "dummy:1234"));

        inputTopic = testDriver.createInputTopic(TOPIC_INPUT_WC, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(TOPIC_OUTPUT_WC, new StringDeserializer(), new LongDeserializer());
    }


    @Test
    public void testLowerCase() {
        String firstRecord = "Kafka KAFKA kafka";
        pushNewRecord("foo", firstRecord);
        TestRecord<String, Long> read1 = readOutput();
        TestRecord<String, Long> read2 = readOutput();
        TestRecord<String, Long> read3 = readOutput();
        assertThat(read1.key()).isEqualTo("kafka");
        assertThat(read2.key()).isEqualTo("kafka");
        assertThat(read3.key()).isEqualTo("kafka");

        assertThat(read3.value()).isEqualTo(3);

        String secondRecord ="Testing Kafka again";
        pushNewRecord("bar", secondRecord);
        TestRecord<String, Long> read4 = readOutput();
        TestRecord<String, Long> read5 = readOutput();
        TestRecord<String, Long> read6 = readOutput();

        assertThat(read4.key()).isEqualTo("testing");
        assertThat(read5.key()).isEqualTo("kafka");
        assertThat(read6.key()).isEqualTo("again");

        assertThat(read5.value()).isEqualTo(4);
    }

}
