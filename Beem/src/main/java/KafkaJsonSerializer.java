import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Creating a Custom serializer so we can serialize the RideShare and DeliveryJob objects into JSON.
 * This will make it easier for when we need to route jobs to different topics using the Streams API.
 * @param <T> The Java POJO object we need to serialize into a json string
 */
public class KafkaJsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public KafkaJsonSerializer() {
    }


    public void configure(Map<String, ?> props, boolean isKey) {}


    /**
     * Serializes the java pojo object into a json byte array.
     * @param topic
     * @param data
     * @return
     */
    public byte[] serialize(String topic, T data)
    {
        if (data == null)
            return null;

        try
        {
            return objectMapper.writeValueAsBytes(data);
        }
        catch (Exception e)
        {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    public void close() {}

}



// import org.apache.kafka.common.serialization.Serializer;
//import org.apache.kafka.common.serialization.Deserializer;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Map;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//public class KafkaJsonSerializer implements Serializer<JsonNode>, Deserializer<JsonNode>
//{
//    private Logger logger = LoggerFactory.getLogger(this.getClass());
//    private ObjectMapper mapper = new ObjectMapper();
//
//    //
//    public byte[] serialize(String topic, JsonNode data)
//    {
//        try
//        {
//            return mapper.writeValueAsBytes(data);
//        }
//        catch (JsonProcessingException e)
//        {
//            return new byte[0];
//        }
//    }
//
//    //
//    public JsonNode deserialize(String topic, byte[] data)
//    {
//        try
//        {
//            return mapper.readValue(data, JsonNode.class);
//        }
//        catch (IOException e)
//        {
//            logger.error(e.getMessage());
//        }
//
//        return null;
//    }
//
//    //
//    public void configure(Map<String, ?> configs, boolean isKey) {}
//
//    //
//    public void close() {}
//
//}
