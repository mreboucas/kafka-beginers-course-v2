package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer01 {

    Logger logger = LoggerFactory.getLogger(TwitterProducer01.class.getName());

    String consumerKey = "eA8ULFNWh0RBXaccMZXhbAmS8";
    String consumerSecret = "yBBu6o85Fw0Pi7LEGOzQEWjFJW4BAJkVCfmXzcS81D6Z8qiCg1";
    String token = "1372314721018384390-uyYQMNdmuFhSRgMN5UvTUkZiQWoem4";
    String secret = "yeIUiRPrYtTkQOBDwtSFhUF2EwwsOGMlqQmcaX9KdaPWw";

    List<String> terms = Lists.newArrayList("bitcoin", "covid", "brasil", "ps5", "ps4");

    TwitterProducer01() {}

    public static void main(String[] args) {
        new TwitterProducer01().run();
    }

    public void run() {
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //Create a twitter client
        Client client = createATwitterClient(msgQueue);
        //Attempts to establish a connection
        client.connect();

        //Create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done");
        }));

        //Loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                //Puxa as msgs do twtier a cada 5 segundos
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    public Client createATwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //professor comentou
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //professor comentou
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-1")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        //All === -1 in those kafka version
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        //Kafka > 2.0 -> supports 5. Use 1 otherwise kafka versions
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer (at the expense of a bit of latency and CPU usage)
        //-> snappy: good balance for CPU compression
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //-> delay for until message bytes size config not complete yet.
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        //-> size of message
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 kb batch size


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }
}
