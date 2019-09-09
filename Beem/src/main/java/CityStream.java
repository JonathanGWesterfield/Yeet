import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

/**
 * A streaming application that looks at
 */
public class CityStream
{
    public static void main(String[] args)
    {

    }

    private String schoolTopic;
    private int topicPartition;


    /**
     * Default Constructor
     */
    public CityStream(String schoolTopic, int topicPartition)
    {
        this.schoolTopic = schoolTopic;
        this.topicPartition = topicPartition;
    }
}
