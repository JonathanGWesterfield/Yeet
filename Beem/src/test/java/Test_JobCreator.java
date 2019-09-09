import junit.framework.TestCase;
import static org.junit.Assert.*;
import org.junit.Test;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;

public class Test_JobCreator
{
    String topicName = "texas-am-university_delivery_jobs";

    @Test
    public void sendTestMessage()
    {
        JobCreator job = new JobCreator(topicName);
        SimpleConsumer simCon = new SimpleConsumer(topicName);

        ConsumerRecords<String, String> consumerRecords = simCon.getConsumer().poll(1000);
        job.sendTestMessage();

        for (ConsumerRecord<String, String> record : consumerRecords) {
            System.out.printf("Consumer Record:(%s, %s, %s, %s)\n",
                    record.key(), record.value(),
                    record.partition(), record.offset());
        }

    }
}
