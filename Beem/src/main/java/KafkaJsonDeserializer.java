import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Creating a Custom deserializer so we can deserialize the RideShare and DeliveryJob objects from JSON.
 * This will make it easier for when we need to route jobs to different topics using the Streams API.
 * @param <T> The JSON object we need to deserialize back into a java POJO.
 */
public class KafkaJsonDeserializer<T> implements Deserializer<T>
{
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    /**
     * Constructor. Takes the class type for the object we need to deserialize our Json into
     * @param type
     */
    public KafkaJsonDeserializer(Class type)
    {
        this.tClass = type;
    }

//    @SuppressWarnings("unchecked")
//    public void configure(Map<String, ?> props, boolean isKey)
//    {
//        // tClass = (Class<T>) props.get("JsonPOJOClass");
//    }

    @SuppressWarnings("unchecked")
    public void configure(Map map, boolean b) {}

    public T deserialize(String topic, byte[] bytes)
    {
        if (bytes == null)
            return null;

        T data;
        try
        {
            data = objectMapper.readValue(bytes, tClass);
        }
        catch (Exception e)
        {
            throw new SerializationException(e);
        }

        return data;
    }

    public void close() {}
}