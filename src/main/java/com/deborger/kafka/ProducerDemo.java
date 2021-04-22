package com.deborger.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
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
        producer.send(producerRecord);
        // flush data
        producer.flush();
        // flush and close
        producer.close();
    }
}
