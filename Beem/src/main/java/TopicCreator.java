import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import java.lang.Throwable;

import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.fasterxml.jackson.databind.util.ClassUtil.getRootCause;

/**
 * This class is a helper to create kafka topics as needed without having to have an admin go to the console
 * and create one manually. This is for when we are using data streams and need to move information to a city or school
 * that doesn't exist in our system yet. Obviously this will need to be tweaked as the system architecture changes over time.
 *
 *
 * @author Jonathan Westerfield
 * @version 1.0
 * @since 9/19/2019
 */
public class TopicCreator
{
    private String zookeeperConnect = "zkserver1:2181";
    private String kakfaBrokerAddress = "localhost:9092";
    private NewTopic topic;
    private int sessionTimeoutMs = 10 * 1000; // 10 Second timeout
    private int connectionTimeoutMs = 8 * 1000; // 8 second timeout

    public static void main(String[] args)
    {
        TopicCreator newTopic = new TopicCreator("localhost:2181", "localhost:9092");
        newTopic.createTopic("dallas-tx-delivery", 1, 1);

        HashMap<String, String> configs = newTopic.getNewTopicInfo();

    }

    /**
     * Default empty constructor
     */
    public TopicCreator()
    { /* Default Constructor */}

    /**
     * Constructor for this class. This means that there should be different topic creators depending on the broker
     * address we are trying to create a topic on.
     * @param zookeeperAddress The zookeeper address for the whole system (or one of them).
     * @param brokerAddress The address of the broker we are trying to create a new topic on.
     */
    public TopicCreator(String zookeeperAddress, String brokerAddress)
    {
        this.zookeeperConnect = zookeeperAddress;
        this.kakfaBrokerAddress = brokerAddress;
    }

    /**
     * Create the connection properties needed to connect to the broker but also the properties needed to create
     * a new topic on the broker.
     * @param brokerAddress The address of the broker we are trying to create a new topic on.
     * @return The properties that will allow us to connect to and create a new topic on the specified broker.
     */
    public Properties configProps(String brokerAddress)
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

        return config;
    }

    /**
     * Will create a new topic with number of partitions and replications specified by the function parameters.
     * Will create the topic on the kafka broker specified in the constructor.
     * @param topicName The name of the new topic we want to create
     * @param numPartitions The number of partitions this topic will be split into.
     * @param numReplication The number of times this topic (and its partitions) will be replicated to make sure it is fault tolerant.
     * @see 'https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html'
     */
    public void createTopic(String topicName, int numPartitions, int numReplication)
    {
        AdminClient admin = AdminClient.create(configProps(this.kakfaBrokerAddress));

        Map<String, String> configs = new HashMap<>();

        this.topic = new NewTopic(topicName, numPartitions, (short)numReplication);

        CreateTopicsResult result = admin.createTopics(Arrays.asList(topic.configs(configs)));

        logTopicCreation(result);
    }

    /**
     * Gives us information on the topic we just created. This is to store in a database in case we need
     * to reference it for other streaming or consumer applications.
     * @return Information about the topic in key value pairs.
     */
    public HashMap<String, String> getNewTopicInfo()
    {
        // Case where user is calling this before creating the new topic
        if(this.topic == null)
            throw new NullPointerException("You must call createTopic() before you can get the new topic details!!");

        HashMap<String, String> props = new HashMap<>();
        props.put("name", topic.name());
        props.put("numPartitions", Integer.toString(topic.numPartitions()));
        props.put("replicationFactor", Short.toString(topic.replicationFactor()));
        props.put("replicasAssignments", topic.replicasAssignments().toString());
        props.put("configs", topic.configs().toString());

        return props;
    }

    /**
     * Logs the success or failure of creating the topic. Currently it just prints it out but this should use
     * whatever logging tool we are using in the future.
     * @param result The result of the topic creation from admin.createTopics()
     */
    public void logTopicCreation(CreateTopicsResult result)
    {
        for(Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet())
        {
            try
            {
                entry.getValue().get();
                // TODO: CHANGE THIS TO A LOGGING EVENT
                System.out.printf("Topic %s created", entry.getKey());
            }
            catch (InterruptedException | ExecutionException e)
            {
                if(getRootCause(e) instanceof TopicExistsException)
                {
                    // TODO: CHANGE THIS TO A LOGGING EVENT
                    System.out.printf("Topic %s already existed!", entry.getKey());
                }
            }
        }
    }

    // TODO: PUT THE NEW TOPIC NAME INTO A DATABASE OF EXISTING KAFKA TOPICS FOR FUTURE LOOKUP

    //TODO: TEST THIS TO MAKE SURE IT WORKS
}
