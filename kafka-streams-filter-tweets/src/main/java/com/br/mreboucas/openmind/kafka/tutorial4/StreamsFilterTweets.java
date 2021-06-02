package com.br.mreboucas.openmind.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        /**Estrutura simular ao consumer groups, mas este é para fluxo(stream)**/
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        /**Serealização/deserialização**/
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input the topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter for tweets wich has a user of over 10000 followers
                //Lista os usuários com possuem mais de 10000 seguidores
                (k,jsonTweet) -> extractUserFollwersInTweet(jsonTweet) > 10000
               );

        filteredStream.to("important_tweets");


        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static int extractUserFollwersInTweet(String tweetJson){
        // gson library
        try {

        return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();

        } catch(NullPointerException ex) {
            return 0;
        }
}
}
