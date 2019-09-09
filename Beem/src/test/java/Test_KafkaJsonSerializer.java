import junit.framework.TestCase;
import static org.junit.Assert.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

public class Test_KafkaJsonSerializer
{
    @Test
    public void testSerialize()
    {
        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", "Medium");

        Properties props = new Properties();
        // Producer<String, DeliveryJobPOJO> producer = new KafkaProducer<String, DeliveryJobPOJO>(props, new StringSerializer(), new KafkaJsonSerializer());

        //TODO: FINISH TESTING THE PRODUCTION AND CONSUMPTION OF THE JSON SERIALIZER
    }
}
