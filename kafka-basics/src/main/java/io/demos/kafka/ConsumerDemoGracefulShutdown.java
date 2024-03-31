package io.demos.kafka;

import com.common.constants.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoGracefulShutdown {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoGracefulShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am Kafka Consumer");
        List<String> topics = new ArrayList<>();

        String topic = KafkaConstants.Topic_ID;
        topics.add(topic);

        //connect to local host
        Properties property = new Properties();
        property.setProperty("bootstrap.servers", "127.0.0.1:9092");


        // create producer properties
        property.setProperty(KafkaConstants.DeserializerKey, StringDeserializer.class.getName());
        property.setProperty(KafkaConstants.DeserializerValue, StringDeserializer.class.getName());
        property.setProperty(KafkaConstants.GroupIDProperty, KafkaConstants.groupID);
        property.setProperty(KafkaConstants.auto_Offset_Reset_Property, KafkaConstants.offset_property_Earliest);

        //create a kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(property);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling the consumer.wakeup()");
                consumer.wakeup();

                //join the main thread to all the execution of the clode in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            //subscrible to a topic
            consumer.subscribe(topics);

            //poll for data
            while (true) {

                log.info("polling");

                //how long are we willing to wait to receive data
                //if there is not data, it will wait 1sec to receive data from kafka
                //once the wakeup() is called from the shutdown hook, consumer.poll will throw the wakeupexception
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("Partition: " + record.partition() + ", offset: " + record.offset());
                }
            }


        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception catch" + e);
        } finally {
            consumer.close(); //close the consumer, will also commit the offsets
            log.info("The consumer has gracefully shutdown");
        }
    }
}
