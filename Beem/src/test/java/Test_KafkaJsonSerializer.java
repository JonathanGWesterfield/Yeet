import junit.framework.TestCase;
import static org.junit.Assert.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.*;

import java.util.Collections;
import java.util.Properties;

public class Test_KafkaJsonSerializer
{
    @Test
    public void testSerialize()
    {
        String topic = "texas-am-university";

        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", DeliveryJob.Sizes.MEDIUM);

        Properties props = configProps();
        Producer<String, DeliveryJob> producer =
                new KafkaProducer<String, DeliveryJob>(
                        props,
                        new StringSerializer(),
                        new KafkaJsonSerializer()
                );

        // Send a message to the topic
        producer.send(new ProducerRecord<String, DeliveryJob>(topic, "0", job));

        // Get that message from the topic, deserialize it and test that it is equal (has the same contents)
        Consumer<String, DeliveryJob> consumer =
                new KafkaConsumer<String, DeliveryJob>(
                        configConsumeProps(),
                        new StringDeserializer(),
                        new KafkaJsonDeserializer<DeliveryJob>(DeliveryJob.class)
                );

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic));

        DeliveryJob recordJob = new DeliveryJob();

        ConsumerRecords<String, DeliveryJob> records = consumer.poll(1000);

        for (ConsumerRecord<String, DeliveryJob> record : records)
        {
            if(record.value() != null)
            {
                System.out.println(record.value());
                // System.out.println(record.value());
                recordJob = record.value();
            }
        }

        consumer.commitAsync();

        assertEquals(true, job.equals(recordJob));

        // consumer.commitAsync();

        consumer.close();
        System.out.println("DONE");


    }

    public Properties configConsumeProps()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "texas-am-university_delivery_jobs");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");

        return props;
    }

    public Properties configProps()
    {
        Properties props = new Properties();

        // Assign localhost ID
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests
        props.put("acks", "all");

        //if the request fails, the producer can automatically retry
        props.put("retries", 0);

        // Specify the buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    /* Reference Consumer Code

    //        // Number of times to look at the server before we give up on any messages coming through
//        int giveUp = 4;
//        int noRecordsCount = 0;
//        while (true) {
//            // Run the consumer to consume the message we just sent and verify it can be deserialized back into a java POJO
//            ConsumerRecords<String, DeliveryJob> records = consumer.poll(1000);
//
//            if (records.count() == 0) {
//                noRecordsCount++;
//                System.out.println("No Records, trying again");
//                if (noRecordsCount > giveUp) break;
//                else continue;
//            }
//
////            records.forEach(record -> {
////                System.out.printf(record.value().getTo_address());
////                // recordJob = record.value();
////            });
//            for (ConsumerRecord<String, DeliveryJob> record : records)
//            {
//                if(record.value() != null)
//                {
//                    System.out.println(record.value());
//                    recordJob = record.value();
//                }
//            }
//
//            consumer.commitAsync();
//        }
     */
}
