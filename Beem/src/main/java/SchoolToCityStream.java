import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.util.JSONPObject;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.json.*;
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
    private Map<String, List<PartitionInfo>> cityKTopics;
    private StreamsBuilder builder;

    private KStream<String, String> sourceStream;
    private KStream<String, String> deliveryStream;
    private KStream<String, String> rideShareStream;
    private KStream<String, String>[] cityStreams; // array of streams - one stream for each city

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
        this.cityKTopics = getKTopics(cityBrokerAddress);

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
                        JSONObject json = (JSONPObject) new JSONParser().parse(value);
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
                        JSONObject json = (JSONPObject) new JSONParser().parse(value);
                        String jobType = json.get("job_type").toString();

                        if(jobType.equalsIgnoreCase("rideShare"))
                            return true;

                        return false;
                    }
                }
        );
    }



    /**
     * Setter and validator for the school topic we are going to be streaming data from. Throws an exception
     * if the topic does not exists on the specified broker.
     * @param schoolTopic The topic we need to verify actually exists before pulling data from it.
     * @throws NoSuchElementException Throws if topic does not exist at the specified broker.
     */
    public void setSchoolTopic(String brokerAddress,String schoolTopic) throws NoSuchElementException
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
     * to stream from actually exists.
     */
    public Map<String, List<PartitionInfo>> getKTopics(String brokerAddress)
    {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
//        props.put("group.id", this.schoolTopic);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();

        return consumer.listTopics();
    }



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
