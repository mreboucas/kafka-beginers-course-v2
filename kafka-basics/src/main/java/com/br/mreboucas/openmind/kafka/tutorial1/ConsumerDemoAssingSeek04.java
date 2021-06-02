package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
 *
 * @INFO: //Assing and seek are mostly used to  replay data of fetch a specifi messsage
 * Essa abordagem é peculiar e menos utilizada.
 */
    public class ConsumerDemoAssingSeek04 {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssingSeek04.class);

        String bootstrapServers = "localhost:9092";
        String offsetResetConfig[] = {"earliest","latest","none"};
        String topic = "first_topic";
        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetResetConfig[0]);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assing and seek are mostly used to  replay data of fetch a specifi messsage

        //Assing (assino)
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek (procuro)
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

        //poll for new data
//        while(true) { //bad praticies - only to demonstration
        while(keepOnReading) {
//          consumer.poll(100); //deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord record : records) {
                numberOfMessageReadSoFar += 1;
                logger.info("Key: " + record.key() + "\n"+
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset());
                if (numberOfMessageReadSoFar > numberOfMessagesToRead) {
                    keepOnReading = false; //to exit to while
                    break; //to exit to loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
