import com.sun.tools.classfile.ConstantPool;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is meant to look at the topics of the schools set in the constructor and route the jobs
 * from those topics to their respective cities and city job types. This kind of application is a
 * redirect application.
 *
 * @author Jonathan Westerfield
 * @version 1.0
 * @since 9/19/2019
 */
public class SchoolToCityStream implements Runnable
{
    private String state;
    private String school;
    private String appName;
    private String schoolTopic;
    private String schoolBrokerAddress;
    private String cityBrokerAddress;
    private HashMap<String, String> cities; // used to keep the cities we have in our database and their topic names
    private Map<String, String> cityTopics;

    // No longer used
    private Map<String, List<PartitionInfo>> cityDeliveryKTopics;
    private Map<String, List<PartitionInfo>> cityRideShareKTopics;

    private KafkaConsumer<String, String> schoolConsumer;
    private KafkaProducer<String, String> cityProducer;
    private Topology topology; // the final streams setup when it is complete

    private CountDownLatch latch; // used to kill the process when the time comes
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static void main(String[] args)
    {
        SchoolToCityStream cityStream = new SchoolToCityStream("TX", "Texas A&M University",
                "texas-am-university", "localhost:9092", "localhost:9092", "tx-school-city-redirect");
    }

    // TODO: MAKE THIS CLASS A MULTITHREADED CLASS SO WE CAN RUN MULTIPLE STREAMS FROM ONE MODULE
    // INSTRUCTIONS HERE: 'https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html'

    /**
     * Empty Default Constructor
     */
    public SchoolToCityStream()
    {
        { /* Empty Default Constructor */}
    }

    /**
     * Constructor for the creating a stream that will stream and split data from the topic of a school. For example
     * taking data from the texas-am-university topic and sending the information to the correct city topics like
     * houston, austin, etc. This only splits the data stream from a single school to multiple cities. The state
     * and school are also used to create the application ID for kafka's use and tracking.
     *
     * @param state The state that the school is located in. Two letter code: ex. TX, CA
     * @param school The university/school topic we are pulling data from.
     * @param schoolTopic The school topic we are trying to stream from.
     * @param schoolBrokerAddress The kafka broker topic connection string. Ex. - 'ipaddress:portNum'
     * @param cityBrokerAddress The kafka broker that has the city topic we want. Ex - Ex. - 'ipaddress:portNum'.
     * @param appName The name given to this splitter module. Ex - tx-school-city-redirect
     * @throws NoSuchElementException
     */
    public SchoolToCityStream(String state, String school, String schoolTopic,
                              String schoolBrokerAddress, String cityBrokerAddress, String appName) throws NoSuchElementException
    {
        setState(state);
        this.school = school;
        this.schoolBrokerAddress = schoolBrokerAddress;
        this.cityBrokerAddress = cityBrokerAddress;
        this.appName = appName;

        // Get all of our topics in order so we can direct our messages to them.
        setSchoolTopic(schoolBrokerAddress, schoolTopic);
//        this.brokerKTopics = getKTopics(cityBrokerAddress);
        this.cityTopics = getCityTopics(getKTopics(cityBrokerAddress));

        this.schoolConsumer = new KafkaConsumer<String, String>(getConsumerProps());
        this.cityProducer = new KafkaProducer<String, String>(getProducerProps());


        this.latch = new CountDownLatch(1);

    }

    /**
     * This is the run function that will start when we start the thread. Should split information until a kill
     * signal is sent.
     */
    public void run()
    {
        try
        {
            this.schoolConsumer.subscribe(Arrays.asList(this.schoolTopic));
            while (!closed.get())
            {
                ConsumerRecords records = this.schoolConsumer.poll(Duration.ofMillis(500));

                records.forEach(record -> {
                    String job = ((ConsumerRecord<String, String>) record).value();
                    String key = getKey(job);
                    String topic = getDestinationTopic((job));

                    this.cityProducer.send(new ProducerRecord<String, String>(topic, key, job));
                });

                // Handle new records
            }
        }
        catch (WakeupException e)
        {
            // Ignore exception if closing
            if (!closed.get())
                throw e;
        }
        finally
        {
            this.schoolConsumer.close();
        }
    }

    /**
     * Gets called when we need to shut down this redirect module. Closes the consumer and the producer.
     */
    public void shutdown()
    {
        this.schoolConsumer.close();
        this.cityProducer.close();
        this.schoolConsumer.wakeup();
        this.closed.set(true);
    }

    /**
     * This is a helper to extract the key from the record. The key will be the string representation of the
     * customer id. This can be changed at any point in the future as long as nothing is using the key for
     * special things down stream.
     * @param job The json string representing the job
     * @return The customer id as a string to be sent as the key
     */
    public String getKey(String job)
    {
        JSONObject json = new JSONObject(job);
        return json.get("customer_id").toString();
    }

    /**
     * Uses the city, state, and job type to determine what topic this job should be sent to. This uses the topic
     * naming convention, which is "city-state-job"
     * @param job The json representation of the job message that came from Kafka.
     * @return The topic name that this job should be sent to.
     */
    public String getDestinationTopic(String job)
    {
        JSONObject json = new JSONObject(job);
        String city = json.get("city").toString();
        String stateCode = json.get("state").toString();
        String jobType = json.get("job_type").toString();

        StringBuilder topicName = new StringBuilder();
        topicName.append(city.toLowerCase().trim())
                .append("-")
                .append(stateCode.toLowerCase().trim())
                .append("-")
                .append(jobType.toLowerCase().trim());

        // TODO: NOTIFY USERS DOWNSTREAM THAT THERE IS A NEW TOPIC THAT NEEDS TO BE TRACKED AND CONSUMED
        // If the topic doesn't exist, we need to create a new one and send it there.
        if(!cityTopics.containsKey(topicName.toString()))
        {
            TopicCreator newTopic = new TopicCreator("localhost:2181", "localhost:9092");
            newTopic.createTopic(topicName.toString(), 1, 1);
            this.cityTopics.put(newTopic.getNewTopicName(), "new");
        }

        return topicName.toString();
    }

    /**
     * Sets the properties for us to start consuming messages from the school topic.
     * @return The properties needed to start consuming from the school topic.
     */
    public Properties getConsumerProps()
    {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.schoolBrokerAddress);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.appName);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500"); // consume every 500 milliseconds
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    /**
     * Sets the properties for us to start producing messages after we have processed the messages recieved
     * by the consumer;
     * @return The properties needed to start producing messages for the cities.
     */
    public Properties getProducerProps()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.cityBrokerAddress);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }


    /**
     * Setter and validator for the school topic we are going to be streaming data from. Throws an exception
     * if the topic does not exists on the specified broker.
     * @param schoolTopic The topic we need to verify actually exists before pulling data from it.
     * @throws NoSuchElementException Throws if topic does not exist at the specified broker.
     */
    public void setSchoolTopic(String brokerAddress, String schoolTopic) throws NoSuchElementException
    {
        Map<String, List<PartitionInfo>> existingTopics = getKTopics(brokerAddress);

        if(!existingTopics.containsKey(schoolTopic)) // topic not found
            throw new NoSuchElementException("The topic wasn't found at the broker!");

        this.schoolTopic = schoolTopic;
    }

    /**
     * Setter and validator for the state for this class.
     * @param state The 2 character state code for the state we are setting.
     * @throws IllegalArgumentException Throws if the state was wrong format or doesn't exist.
     */
    public void setState(String state) throws IllegalArgumentException
    {
        if (Address.isValidState(state))
            this.state = state;
        else
            throw new IllegalArgumentException("State must be the 2 character state code! Ex: TX, CA");
    }

    /**
     * Need to get the list of topics from the kafka broker so we can verify that the topic the user wants
     * stream from actually exists.
     * @param brokerAddress The kafka broker that has the city topic we want. Ex - Ex. - 'ipaddress:portNum'.
     * @return A map of the topics. The key is the topic name, the PartitionInfo has the topic name, number of partitions,
     *      the leader, number of replicas, the isr, and info on offline replicas
     */
    public Map<String, List<PartitionInfo>> getKTopics(String brokerAddress)
    {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
//        props.put("group.id", this.schoolTopic);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        consumer.close();

        return topics;
    }

    /**
     * Filters through the Map of topics retrieved from the broker and takes only the topic names and puts
     * them into another hashmap for quick lookups of names.
     * @param kTopics The topics we originally got from the broker.
     * @return A map of the topic names and them all being originally retrieved from the broker, not created in this class.
     */
    public Map<String, String> getCityTopics(Map<String, List<PartitionInfo>> kTopics)
    {
        Map<String, String> map = new HashMap<>();

        for (String topic : kTopics.keySet())
            map.put(topic, "original"); // original since it was retrieved from the broker, not created from here

        return map;
    }

    /**
     * Used to segregate the topics into a delivery topic map for quick look up and categorization of the
     * messages into their respective streams.
     * @param brokerTopics All of the topics that we pulled from the broker.
     * @return All of the city delivery topics we will need to send our new streams to. The key is the 'city-state'.
     *      The rest of the topic information, like the actual topic name is in the PartitionInfo list, the value
     *      of the map.
     */
    protected Map<String, List<PartitionInfo>> getDeliveryTopics(Map<String, List<PartitionInfo>> brokerTopics)
    {
        Map<String, List<PartitionInfo>> topics = new HashMap<>();

        for (Map.Entry<String, List<PartitionInfo>> entry : brokerTopics.entrySet())
        {
            // Format for city topics is 'city-state-job'
            String city, state, job;
            String[] key = entry.getKey().split("-");

            if (key.length < 3) // name not long enough so can't be valid topic
                continue;

            job = key[2];
            if(!isValidJob(job))
                continue; // not a valid job so go to next one

            city = key[0];
            state = key[1];


            if(job.equalsIgnoreCase("delivery"))
                topics.put((city + "-" + state), entry.getValue());
        }

        return topics;
    }

    /**
     * Used to segregate the topics into a delivery topic map for quick look up and categorization of the
     * messages into their respective streams.
     * @param brokerTopics All of the topics that we pulled from the broker.
     * @return All of the rideshare city topics we will need to send our new streams to. The key is the 'city-state'.
     *      The rest of the topic information, like the actual topic name is in the PartitionInfo list, the value
     *      of the map.
     */
    protected Map<String, List<PartitionInfo>> getRideShareTopics(Map<String, List<PartitionInfo>> brokerTopics)
    {
        Map<String, List<PartitionInfo>> topics = new HashMap<>();
        String city, state, job;

        for (Map.Entry<String, List<PartitionInfo>> entry : brokerTopics.entrySet())
        {
            // Format for city topics is 'city-state-job'
            String[] key = entry.getKey().split("-");

            if (key.length < 3) // name not long enough so can't be valid topic
                continue;

            job = key[2];
            if(!isValidJob(job))
                continue; // not a valid job so go to next one

            city = key[0];
            state = key[1];


            if(job.equalsIgnoreCase("rideshare"))
                topics.put((city + "-" + state), entry.getValue());
        }

        return topics;
    }

    /**
     * Helper function to help is determine if a job is a valid one. Need this for looking at topics to ensure that
     * we are storing actual city topics instead of school topics.
     * @param job The job we are trying to validate. Ex - delivery
     * @return True if the job string is a valid job type.
     */
    private boolean isValidJob(String job)
    {
        if (job.equalsIgnoreCase("delivery") || job.equalsIgnoreCase("rideshare"))
            return true;
        return false;
    }
}










// Ditching this code because I fundamentally misunderstood what streams are for. They are mostly for data
// Transformation, not routing.

// /**
 //     * Create the properties for this streaming application. This is by default setup for only looking
 //     * at school topics in Texas.
 //     * @return The configuration properties to connect to our brokers.
 //     */
//    public Properties getStreamProps()
//    {
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tx-school-city-stream");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        return props;
//    }

//    /**
//     * Calls the jobs splitter and then the city splitter stream functions in order to build the streaming topology out.
//     */
//    private Topology buildTopology()
//    {
//        setUpJobStreams();
//
//        // setUpCityStreams();
//
//        return this.builder.build();
//    }
//
//    /**
//     * Sets up the 2 streams that stem from the school stream. The 2 streams will split into stream pertaining
//     * to the delivery and ride share job streams respectively. Once split, the streams will need to be split
//     * again based on the location of where the job starts from.
//     */
//    private void setUpJobStreams()
//    {
//        this.sourceStream = builder.stream(this.schoolTopic);
//
//        // Filter to get all of the jobs that are delivery jobs
//        this.deliveryStream = this.sourceStream.filter(
//                new Predicate<String, String>()
//                {
//                    @Override
//                    public boolean test(String key, String value)
//                    {
//                        JSONObject json = new JSONObject(value);
//                        String jobType = json.get("job_type").toString();
//
//                        if(jobType.equalsIgnoreCase("delivery"))
//                            return true;
//
//                        return false;
//                    }
//                }
//        );
//
//        // Filter to get all of the jobs that are rideshare jobs
//        this.rideShareStream = this.sourceStream.filter(
//                new Predicate<String, String>() {
//                    @Override
//                    public boolean test(String key, String value)
//                    {
//                        JSONObject json = new JSONObject(value);
//                        String jobType = json.get("job_type").toString();
//
//                        if(jobType.equalsIgnoreCase("rideShare"))
//                            return true;
//
//                        return false;
//                    }
//                }
//        );
//    }
//
//    //TODO: FIGURE OUT HOW TO FUCKING BRANCH THESE EVENTS IN A SANE MANNER
//    /**
//     * How to branch streams is found here:
//     * @see 'https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#transform-a-stream'
//     * @see 'https://kafka-tutorials.confluent.io/split-a-stream-of-events-into-substreams/kstreams.html'
//     */
//    public void setUpCityStreams()
//    {
//
//
//        this.cityDeliveryStreams = this.deliveryStream.filter(
//                new Predicate<String, String>() {
//                    @Override
//                    public boolean test(String s, String s2) {
//                        return false;
//                    }
//                }
//        );
//    }
//
//    /* Try this. Create a stream arraylist that is based on each city that we can send to. Each of those streams
//        has to send to its specific sink processor (derived from the city topics map). Create a for loop that
//        will go down the list of topics and create a stream for each city with have in our stored city topics.
//        Use source.filter for each one. Create a predicate for each city topic we have.
//    */

