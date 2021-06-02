package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * AULA 05 - Mod 07
 */
public class ProducerDemoWithCallback02 {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback02.class);
        //Create a producer config
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //O kafka por padrão enviará bytes, mas vamos converter os valores e chaves para strings.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        int i=0;
        int stop = 10;
        while (i < stop) {

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello" + i);

            //Send data - asynchonous (tem a necessidade do flush e close)
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time a record is successfully sent or an exceptionis throws

                    if (e == null) {
                        //the record was successfully sent
                        logger.info("\nReceived new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Patition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error producing", e);
                    }
                }
            });
            i++;
        }

        //Flush data
        producer.flush();
        //Flush and close producer
        producer.close();

    }
}