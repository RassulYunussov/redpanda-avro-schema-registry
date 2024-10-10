package org.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:19092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:18081");
        properties.put(KafkaAvroSerializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        properties.put("group.id", "simple-consumer-example");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("compression.type", "snappy");

        try (org.apache.kafka.clients.consumer.Consumer<String, SpecificRecord> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("some-another-topic"));
            while (true) {
                final ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                records.forEach(r -> {
                    switch (r.value()) {
                        case org.example.avro.Customer c -> System.out.println(c);
                        case org.example.avro.Product p -> System.out.println(p);
                        default -> System.out.println("unknown");
                    }
                });
            }
        }
    }
}
