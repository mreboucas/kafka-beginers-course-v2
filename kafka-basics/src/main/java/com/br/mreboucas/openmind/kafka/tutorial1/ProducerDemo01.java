package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo01 {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        //Create a producer config
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //O kafka por padrão enviará bytes, mas vamos converter os valores e chaves para strings.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","hello");

        //Send data - asynchonous (tem a necessidade do flush e close)
        producer.send(record);

        //Flush data
        producer.flush();
        //Flush and close producer
        producer.close();

    }
}