import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.util.*;

/**
 * This class is meant to look at the topics of the schools set in the constructor and route the jobs
 * from those topics to their respective cities and city job types.
 *
 * @author Jonathan Westerfield
 * @version 1.0
 * @since 9/19/2019
 */
public class SchoolToCityStream
{
    private String state;
    private String school;
    private String schoolTopic;
    private String schoolBrokerAddress;
    private String cityBrokerAddress;
    private HashMap<String, String> cities; // used to keep the cities we have in our database and their topic names
    private Map<String, List<PartitionInfo>> schoolKTopics;
    private Map<String, List<PartitionInfo>> brokerKTopics;
    private Map<String, List<PartitionInfo>> cityDeliveryKTopics;
    private Map<String, List<PartitionInfo>> cityRideShareKTopics;
    private StreamsBuilder builder;

    private KStream<String, String> sourceStream;
    private KStream<String, String> deliveryStream;
    private KStream<String, String> rideShareStream;
    private KStream<String, String>[] cityDeliveryStreams; // array of delivery streams - one stream for each city
    private KStream<String, String>[] cityRideShareStream; // array of ride share streams - one stream for each city

    public static void main(String[] args)
    {
        SchoolToCityStream cityStream = new SchoolToCityStream("TX", "Texas A&M University",
                "texas-am-university", "localhost:9092", "localhost:9092");
    }

    // TODO: MAKE THIS CLASS A MULTITHREADED CLASS SO WE CAN RUN MULTIPLE STREAMS FROM ONE MODULE

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
     * @throws NoSuchElementException
     */
    public SchoolToCityStream(String state, String school, String schoolTopic,
                              String schoolBrokerAddress, String cityBrokerAddress) throws NoSuchElementException
    {
        setState(state);
        this.school = school;
        this.schoolBrokerAddress = schoolBrokerAddress;
        this.cityBrokerAddress = cityBrokerAddress;

        setSchoolTopic(schoolBrokerAddress, schoolTopic);
        this.brokerKTopics = getKTopics(cityBrokerAddress);
        this.cityDeliveryKTopics = getDeliveryTopics(this.brokerKTopics);
        this.cityRideShareKTopics = getRideShareTopics(this.brokerKTopics);

        this.builder = new StreamsBuilder();

        setUpJobStreams();



    }

    /**
     * Sets up the 2 streams that stem from the school stream. The 2 streams will split into stream pertaining
     * to the delivery and ride share job streams respectively. Once split, the streams will need to be split
     * again based on the location of where the job starts from.
     */
    private void setUpJobStreams()
    {
        this.sourceStream = builder.stream(this.schoolTopic);

        this.deliveryStream = this.sourceStream.filter(
                new Predicate<String, String>()
                {
                    @Override
                    public boolean test(String key, String value)
                    {
                        JSONObject json = new JSONObject(value);
                        String jobType = json.get("job_type").toString();

                        if(jobType.equalsIgnoreCase("delivery"))
                            return true;

                        return false;
                    }
                }
        );

        this.rideShareStream = this.sourceStream.filter(
                new Predicate<String, String>() {
                    @Override
                    public boolean test(String key, String value)
                    {
                        JSONObject json = new JSONObject(value);
                        String jobType = json.get("job_type").toString();

                        if(jobType.equalsIgnoreCase("rideShare"))
                            return true;

                        return false;
                    }
                }
        );
    }

    //TODO: FIGURE OUT HOW TO FUCKING BRANCH THESE EVENTS IN A SANE MANNER
//    /**
//     * How to branch streams is found here:
//     * @see 'https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#transform-a-stream'
//     * @see 'https://kafka-tutorials.confluent.io/split-a-stream-of-events-into-substreams/kstreams.html'
//     */
//    public void setUpCityStreams()
//    {
//        this.cityDeliveryStreams = this.deliveryStream.branch(
//                new Ks
//        );
//    }



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


    /**
     * Create the properties for this streaming application. This is by default setup for only looking
     * at school topics in Texas.
     * @return The configuration properties to connect to our brokers.
     */
    public Properties createProps()
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tx-school-city-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }


}
