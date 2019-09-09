import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.SocketImpl;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer
{
    private String TOPIC;
    private final String BOOTSTRAP_SERVERS = "localhost:9092";
    private Consumer consumer;

    public SimpleConsumer(String topic)
    {
        this.TOPIC = topic;
        createConsumer(this.TOPIC);
    }

    public Consumer getConsumer() {
        return consumer;
    }

    private Consumer<String, String> createConsumer(String topic)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        this.consumer = consumer;
        return consumer;
    }
}
