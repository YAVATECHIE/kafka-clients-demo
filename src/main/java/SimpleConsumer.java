import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    private static final String ORDERS_TOPIC = "orders";

    public static void main(String[] args) {
        Properties props = new Properties();

        try (InputStream inputStream =
                     SimpleConsumer.class.getClassLoader()
                             .getResourceAsStream("client.properties")) {

            if (inputStream == null) {
                throw new RuntimeException("client-cluster5.properties not found");
            }

            props.load(inputStream);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to load client-cluster5.properties", e);
        }

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "simple-consumer-group");

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(ORDERS_TOPIC));

            while (true) {
                System.out.println("Polling...");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));
                System.out.println("Records fetched = " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Topic=%s Key=%s Value=%s Partition=%d Offset=%d%n",
                            record.topic(),
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset());

                    System.out.println("Processing...");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

    }
}
