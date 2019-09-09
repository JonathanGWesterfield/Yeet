import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonSerializer implements Serializer<JsonNode>, Deserializer<JsonNode>
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ObjectMapper mapper = new ObjectMapper();

    //    @Override
    public byte[] serialize(String topic, JsonNode data)
    {
        try
        {
            return mapper.writeValueAsBytes(data);
        }
        catch (JsonProcessingException e)
        {
            return new byte[0];
        }
    }

    //    @Override
    public JsonNode deserialize(String topic, byte[] data)
    {
        try
        {
            return mapper.readValue(data, JsonNode.class);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }

        return null;
    }

    //    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    //    @Override
    public void close() {}

}
