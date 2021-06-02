package com.br.mreboucas.openmind.kafka.tutorial3;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
//Verificar os offsets das partições
//kafka-consumer-groups --describe --group kafka-demo-elasticsearch --bootstrap-server localhost:9092

/**
 * EXception about consumer was kicked from the group
 * Exception in thread "main" org.apache.kafka.clients.consumer.CommitFailedException: Offset commit cannot be completed since the consumer is not part of an active group for auto partition assignment; it is likely that the consumer was kicked out of the group.
 * Ler sobre os tempos nesse link (ms)
 * https://hackernoon.com/kafka-says-it-is-likely-that-the-consumer-was-kicked-out-of-the-group-az1r34dm
 */
public class ElasticSearchConsumer02 {

    static String TOPIC_TWITTER = "twitter_tweets";
    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer02.class.getName());

    public static RestHighLevelClient createClient() {

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        //Shoud you remove protocol http and port, if exists. They are above, into new HttpHost();
        String hostname = "kafka-basics-cluster-3887026340.us-east-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "6j01d5vxme"; // needed only for bonsai
        String password = "nnwbh5187v"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        //Cria o consumidor KAFKA
        KafkaConsumer<String, String> consumer = createConsumer(TOPIC_TWITTER);

        //poll for new data - envia para o elasticsearch
        while (true) { //bad praticies - only to demonstration
//          consumer.poll(100); //deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000)); //new in kafka 2.0.0
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            //bulk is from the ELK - realizar processamento em lote par o envio ao kafka.
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord record : records) {

                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                TweetDto tweetDto = objectMapper.readValue(String.valueOf(record.value()), TweetDto.class);

                //tWO strategies to generates id idempodente
                //1) Kafka generic ID
                String id = record.topic()  + "-" + record.partition() + "-" + record.offset();
                logger.info("ID gerado: " + id);
                //2) Extract id from your object -> id, pk, etc
                
                //ELK config.
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id
                ).source(objectMapper.writeValueAsString(tweetDto), XContentType.JSON);

                /**
                 * @INFO BulkRequest - replace all code above by bulkrequest only
                 * bulkrequest -> processamento em lote para ao tópico.
                 */
                  bulkRequest.add(indexRequest); //we add to our bulkrequest(takes no time)
//                //Where we insert data into elasticsearch.
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                String idELK = indexResponse.getId();
//                logger.info(idELK);
//
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //Close the client
        //client.close();
    }

        public static KafkaConsumer<String, String> createConsumer (String topic){

            String bootstrapServers = "localhost:9092";
            String groupId = "kafka-demo-elasticsearch";
            String offsetResetConfig[] = {"earliest", "latest", "none"};

            //Create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig[0]);
            //disable autocommit of offsets.
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            //Get 10 records from the kafka topic by request
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

            //create consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(TOPIC_TWITTER));

            return consumer;

        }
    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
