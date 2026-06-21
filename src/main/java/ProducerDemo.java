import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.util.Properties;


public class ProducerDemo {

    private static final String ORDERS_TOPIC = "orders";

    public static void main(String[] args) {


        Properties props = new Properties();

        try (InputStream inputStream =
                     ProducerDemo.class.getClassLoader()
                             .getResourceAsStream("client.properties")) {

            if (inputStream == null) {
                throw new RuntimeException(
                        "client.properties not found");
            }

            props.load(inputStream);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load client.properties",
                    e);
        }

        // Producer-specific settings
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all");

        props.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true);

        // 2. Create producer
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props)) {

            // 3. Send records
            send(producer,
                    ORDERS_TOPIC,
                    "customer-101",
                    "OrderCreated");

            send(producer,
                    ORDERS_TOPIC,
                    "customer-101",
                    "OrderValidated");

            send(producer,
                    ORDERS_TOPIC,
                    "customer-101",
                    "OrderShipped");

            send(producer,
                    ORDERS_TOPIC,
                    "customer-202",
                    "OrderCreated");

            producer.flush();
        }
    }

    private static void send(
            KafkaProducer<String, String> producer,
            String topic,
            String key,
            String value) {

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        topic,
                        key,
                        value);

        // Asynchronous send callback
        producer.send(record,
                (metadata, exception) -> {

                    if (exception == null) {
                        System.out.printf(
                                "Topic=%s Key=%s Value=%s Partition=%d Offset=%d%n",
                                topic,
                                key,
                                value,
                                metadata.partition(),
                                metadata.offset());
                    }
                    else {
                        System.err.printf(
                                "Failed to send record with key=%s, value=%s. Error=%s%n",
                                key,
                                value,
                                exception.getMessage());
                    }
                });
    }
}