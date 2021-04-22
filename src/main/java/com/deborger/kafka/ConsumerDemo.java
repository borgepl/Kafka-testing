package com.deborger.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        System.out.println("Starting the Consumer ...");

        String bootstrapServers = "s-eacea-testpdb:9092";
        String groupId = "my-test-application";
        String offsetReset = "earliest";
        String topic = "firsttopic";

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        // create Consumer configs
        Properties properties =  new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetReset);

        // create the consumer
        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<String, String>(properties);

        // Subscribe Consumer to topic
        // consumer.subscribe(Collections.singleton(topic)); // only to one topic
        consumer.subscribe(Arrays.asList(topic)); // subscribe to multiple topics

        // poll for new data
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record : records) {
                logger.info("Key : " + record.key() + "- Value : " + record.value());
            }
        }
    }
}
