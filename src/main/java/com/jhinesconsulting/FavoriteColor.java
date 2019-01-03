package com.jhinesconsulting;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FavoriteColor {
    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application-3");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "54.215.210.161:9092");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> usersAndColorsPlainTextInput = builder.stream("users-and-colors-plaintext");

        final KStream<String, String> userNameAsKeyColorAsValue = usersAndColorsPlainTextInput
                // 1. Generate a key based on the username
                .selectKey((key, value) -> value.split(",")[0])
                // 2. Map values so that they contain only colors
                .mapValues(color -> color.split(",")[1]);

        userNameAsKeyColorAsValue.to("username-key-color-value", Produced.with(Serdes.String(), Serdes.String()));

        final KTable<String, String> usersAndColorsTable = builder.table("username-key-color-value");

        final KTable<String, Long> colorCounts = usersAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        colorCounts.toStream().to("color-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
