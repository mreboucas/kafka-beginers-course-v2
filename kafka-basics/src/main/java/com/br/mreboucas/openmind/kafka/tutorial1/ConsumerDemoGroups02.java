package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Doc do consumer config - kafka
 *
 * https://kafka.apache.org/documentation/#consumerconfigs
 */
public class ConsumerDemoGroups02 {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups02.class);

        String bootstrapServers = "localhost:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";
        String offsetResetConfig[] = {"earliest","latest","none"};

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetResetConfig[0]);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        //consumer.subscribe(Collections.singleton(topic));
        //Many topics
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true) { //bad praticies - only to demonstration
//          consumer.poll(100); //deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord record : records) {
                logger.info("Key: " + record.key() + "\n"+
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset());
            }
        }
    }
}
