package io.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKey.class.getSimpleName());

    public static void main(String[] args) {

        log.info("hello world");


        //connect to local host
        Properties property = new Properties();
        property.setProperty("bootstrap.servers", "127.0.0.1:9092");


        // create producer properties
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        //create a producer record
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "java_demo";
                String key = "id_" + i;
                String value = "learning Kafka from java" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //execute everytime the record is sent to the producer or a exception is thrown
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Key: " + key + "\n" +
                                    "partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n");
                        } else {
                            log.error("Exception thrown while adding: " + e);
                        }


                    }
                });
            }
        }


//flush tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //close the producer
        producer.close();
    }
}
