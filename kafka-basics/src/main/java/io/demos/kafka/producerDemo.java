package io.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemo {

    private static final Logger log = LoggerFactory.getLogger(producerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("hello world");


        //connect to local host
        Properties property = new Properties();
        property.setProperty("bootstrap.servers", "127.0.0.1:9092");


        // create producer properties
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());

        property.setProperty("batch.size","400");

        //Inefficient
        //property.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        System.out.println(RoundRobinPartitioner.class);

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        //create a producer record
        for (int j = 0; j < 15; j++) {
            for (int i = 0; i < 30; i++) {

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("java_demo", "learning Kafka from java" + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //execute everytime the record is sent to the producer or a exception is thrown
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "TimeStamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Exception thrown while adding: " + e);
                        }

                    }
                });
                System.out.println("j Counter"+ j);
            }

        }


//flush tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //close the producer
        producer.close();
    }
}
