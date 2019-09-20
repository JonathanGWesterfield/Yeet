import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Properties;

public class SchoolToCityStream
{
    private String state;
    private String school;
    private HashMap<String, String> cities; // used to keep the cities we have in our database and their topic names

    public static void main(String[] args)
    {

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
     * @param state The state that the school is located in. Two letter code: ex. TX, CA
     * @param school The university/school topic we are pulling data from.
     */
    public SchoolToCityStream(String state, String school)
    {
        setState(state);
        this.school = school;

        // attach shutdown handler to catch control-c
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }

    }

    /**
     * Setter and validator for the state for this class.
     * @param state The 2 character state code for the state we are setting.
     */
    public void setState(String state)
    {
        if (Address.isValidState(state))
            this.state = state;
        else
            throw new IllegalArgumentException("State must be the 2 character state code! Ex: TX, CA");
    }



    public Properties createProps()
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tx-school-city-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //TODO: Change the serializers once I get the custom JSON serializer working.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }


}
