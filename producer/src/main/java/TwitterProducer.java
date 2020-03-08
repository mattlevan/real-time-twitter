import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Matthew Levan <matt.s.levan@gmail.com>
 * @version 1.0-SNAPSHOT
 * 
 * TweetProducer is run from the command line with a list of keywords passed in 
 * as arguments: `mvn exec:java -Dexec.mainClass=TweetProducer -Dargs="kw1 kw2 kw3"`
 * 
 * It is responsible for running TweetReporter threads, which connect to Twitter 
 * and send new tweets containing the given keywords to a Kafka-compatible Azure Event 
 * Hub specified by TOPIC and configured in src/main/resources/producer.config.
 */
public class TwitterProducer {
    private final static String TOPIC = "twitter-3m-text";
    private final static int NUM_THREADS = 1;

    public static void main(String... args) throws Exception {
        final List<String> keywords = Arrays.asList(args);
        final Producer<String, String> producer = createProducer();
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++)
            executorService.execute(new TwitterReporter(producer, TOPIC, keywords));
    }

    private static Producer<String, String> createProducer() {
        try {
            Properties properties = new Properties();
            properties.load(new FileReader("src/main/resources/confluent-producer.config"));
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "TweetProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            return new KafkaProducer<>(properties);
        }
        catch (Exception e) {
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;
        }
    }
}