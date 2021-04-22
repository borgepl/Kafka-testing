package com.deborger.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        System.out.println("Starting the Producer ...");

        String bootstrapServers = "s-eacea-testpdb:9092";

        // create properties
        Properties properties =  new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create producerRecord
        ProducerRecord<String, String> producerRecord =  new ProducerRecord<>("firsttopic","Hello topic");
        // send data - asynchronous
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // record successfully sent
                    logger.info("Metadata received : \n" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "Timestamp : " + recordMetadata.timestamp());
                    System.out.println(recordMetadata.toString());
                } else {
                    // System.out.println(e.toString());
                    // e.printStackTrace();
                    logger.error("Error while producing : " + e);
                }
            }
        });
        // flush data
        producer.flush();
        // flush and close
        producer.close();
    }
}
