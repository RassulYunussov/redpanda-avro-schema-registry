package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.Customer;
import org.example.avro.Product;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:18081");
        properties.put(KafkaAvroSerializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        properties.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        properties.put("compression.type", "snappy");

        try (final org.apache.kafka.clients.producer.Producer<String, SpecificRecordBase> producer = new KafkaProducer<>(properties)) {
            while(true) {
                org.example.avro.Customer customer = new Customer(1,"customer","some@some.some","some");
                org.example.avro.Product product = new Product(20,"product", 10.10);
                final var futureCustomer = producer.send(new ProducerRecord<>("some-another-topic", "some-key", customer), ((metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("Producing %s resulted in error %s", customer, exception);
                    }
                }));
                final var futureProduct = producer.send(new ProducerRecord<>("some-another-topic", "some-key", product), ((metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("Producing %s resulted in error %s", product, exception);
                    }
                }));
                futureCustomer.get();
                futureProduct.get();
                Thread.sleep(1000);
            }
        }
    }
}
