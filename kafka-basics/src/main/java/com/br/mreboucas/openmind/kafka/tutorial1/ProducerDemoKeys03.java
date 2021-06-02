package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * AULA 06 - Mod 07
 */
public class ProducerDemoKeys03 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "localhost:9092";
        //String groupId = "my-fourth-application";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys03.class);
        //Create a producer config
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //O kafka por padrão enviará bytes, mas vamos converter os valores e chaves para strings.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        int i=0;
        int stop = 10;
        while (i <= stop) {
            String topic = "first_topic";
            String value = "hello word " + i;
            String key = "id_" + i;

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", key, "hello" + i);
            logger.info("Key:" + key);
            //Round robbin para dividir as mensagens nas partições:
            //As mesmas chaves sempre irão para as mesmas partições -
            //O id (key) serve para manter a ordem de execução das mensegans
            //id_0 -> p1
            //id_1 -> p0
            //id_2 -> p2
            //id_3 -> p0
            //id_4 -> p1
            //id_5 -> p2
            //id_6 -> p0
            //id_7 -> p2
            //id_8 -> p1
            //id_9 -> p2
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
            }).get(); //block the .send() to make it synchrosnous- don't do this in production
            i++;
        }

        //Flush data
        producer.flush();
        //Flush and close producer
        producer.close();

    }
}