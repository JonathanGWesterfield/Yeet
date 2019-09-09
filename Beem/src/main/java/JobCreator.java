import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class JobCreator
{
    String topic;
    Producer<String, String> producer;

    /** Main method for testing **/
    public static void main(String[] args)
    {
        JobCreator job = new JobCreator("texas-am-university_delivery_jobs");
        job.sendTestMessage();
    }
    /**
     * Default Constructor
     */
    public JobCreator(String topic)
    {
        // Create a producer with our properties from configProps()
        this.topic = topic;
        this.producer = new KafkaProducer<String, String>(configProps());

        // producer.send(new ProducerRecord<String, String>(topic, )
    }

    public void sendTestMessage()
    {
        Thread.currentThread().setContextClassLoader(null);
        for(int i = 0; i < 10; i++)
        {
            ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic,
                    Integer.toString(i), Integer.toString(i));
            System.out.println(message);
            producer.send(message);
        }
        System.out.println("Message sent successfully");

        producer.close();
    }

    /**
     * Configure the properties that must be set for this producer. These properties are set for a broker
     * running on the local machine
     * @return The properties set for this local machine. This will allow us to send messages to the topic.
     */
    public Properties configProps()
    {
        Properties props = new Properties();

        // Assign localhost ID
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests
        props.put("acks", "all");

        //if the request fails, the producer can automatically retry
        props.put("retries", 0);

        // Specify the buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}
