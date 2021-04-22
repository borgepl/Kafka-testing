package com.deborger.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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
        for (int i=0; i<10; i++ ) {

            String topic = "firsttopic";
            String value = "Msg topic " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key : " + key  );
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
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
            }).get(); // blocks the .send to get it synchronous - not for production !!!!!
        }
        // flush data
        producer.flush();
        // flush and close
        producer.close();
    }
}
