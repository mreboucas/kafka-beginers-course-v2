package com.br.mreboucas.openmind.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @Doc do consumer config - kafka
 *
 * https://kafka.apache.org/documentation/#consumerconfigs
 */
public class ConsumerDemoWithThread03 {


    public static void main(String[] args) {
        new ConsumerDemoWithThread03().run(new ConsumerDemoWithThread03());
    }
    private ConsumerDemoWithThread03() {

    }
    private void run(ConsumerDemoWithThread03 consumerDemoWithThread03) {
        String bootstrapServers = "localhost:9092";
        String groupId = "my-six-application";
        String topic = "first_topic";
        String offsetResetConfig[] = {"earliest","latest","none"};
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread03.class);
        //Latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = consumerDemoWithThread03.new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                null,
                countDownLatch);

        //Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            synchronized (countDownLatch) {
                try {
                    countDownLatch.wait();
                } catch (InterruptedException e) {
                    logger.error("Error", e);
                }
            }
            logger.info("Application has exited");
        }));

        synchronized (countDownLatch) {
            try {
                 countDownLatch.wait();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            } catch (Exception e) {
                logger.error("Error", e);
            } finally {
                logger.info("Applicaton is closing");
            }
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable (String bootstrapServers,
                              String groupId,
                              String topic,
                              String offsetResetConfig,
                              CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;

            //Create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            if(offsetResetConfig != null) {
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetResetConfig);
            }

            //create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            //consumer.subscribe(Collections.singleton(topic));
            //Many topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //poll for new data
                while(true) {
    //          consumer.poll(100); //deprecated
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + "\n"+
                                "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset());
                    }
                }
            } catch (WakeupException  wex){
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                //Tell to main code we're done with the consumer
                countDownLatch.countDown();
            }

        }
        public void shutdown() {
            //The wakeup() method is a special method to interupt consumer.poll()
            //It will throw the exception WakeupException
            consumer.wakeup();

        }
    }
}
