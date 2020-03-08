import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class TwitterConsumerThread implements Runnable {
    private final String TOPIC;
    
    // Each consumer needs a unique client ID per thread
    private static int id = 0;

    public TwitterConsumerThread(final String TOPIC) {
        this.TOPIC = TOPIC;
    }

    public void run() {
        final Consumer<String, String> consumer = createConsumer();
        System.out.println("Polling...");

        try {
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                for (ConsumerRecord<String, String> cr : consumerRecords) {
                    System.out.printf("%s\n", cr.value());
                    // System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", cr.key(), cr.value(), cr.partition(), cr.offset());
                }
                consumer.commitAsync();
            }
        } catch (CommitFailedException e) {
            System.out.println("CommitFailedException: " + e);
        } finally {
            consumer.close();
        }
    }

    private Consumer<String, String> createConsumer() {
        try {
            final Properties properties = new Properties();
            properties.load(new FileReader("src/main/resources/confluent-consumer.config"));
            synchronized (TwitterConsumerThread.class) {
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "TwitterConsumer#" + id);
                id++;
            }
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-3m-wordcount-app");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // Create the consumer using properties
            final Consumer<String, String> consumer = new KafkaConsumer<>(properties);

            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(TOPIC));
            return consumer;
            
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException: " + e);
            System.exit(1);
            return null;        //unreachable
        } catch (IOException e) {
            System.out.println("IOException: " + e);
            System.exit(1);
            return null;        //unreachable
        }
    }
}
