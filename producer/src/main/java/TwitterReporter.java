import com.google.gson.Gson;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterReporter implements Runnable {
    private final Producer<Long, String> producer;
    private final String TOPIC;
    private final List<String> keywords;
    private BlockingQueue<String> queue;
    private Client client;
    private Callback callback;
    private Gson gson;

    public TwitterReporter(final Producer<Long, String> producer, final String TOPIC, final List<String> keywords) {
        // Set instance variables
        this.producer = producer;
        this.TOPIC = TOPIC;
        this.keywords = keywords;

        // Authenticate with the Twitter API
        Authentication authentication = authenticate("src/main/resources/twitter.config");

        // Set the endpoint to track only tweets with the provided keywords
        final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(keywords);

        // Instantiate a linked blocking queue for temporary tweet storage
        queue = new LinkedBlockingQueue<String>(10000);

        // Instantiate a Twitter API client
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Implement an onCompletion method for the Kafka callback
        callback = new Callback() { 
            public void onCompletion(RecordMetadata metdata, Exception e) {
                if (e != null) {
                    System.out.printf("Record could not be produced to %s", TOPIC);
                    System.out.println(e);
                    System.exit(1);
                }
            }
        };

        // Instantiate a Gson object for parsing JSON tweets
        gson = new Gson();
    }

    @Override
    public void run() {
        // Establish a Twitter API connection
        client.connect();

        // Produce tweets
        try {
            while (true) {
                String rawTweet = queue.take();
                Tweet tweet = gson.fromJson(rawTweet, Tweet.class);
                System.out.printf("Fetched tweet id %d from user %s\n", tweet.getId(), tweet.getUser());
                long key = tweet.getId();
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, rawTweet);
                producer.send(record, callback);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
        
        System.out.println("Finished sending tweets with keywords: " + keywords);
    }

    private Authentication authenticate(final String twitterConfFilePath) {
        try {
            Properties properties = new Properties();
            properties.load(new FileReader(twitterConfFilePath));

            final Authentication authentication = new OAuth1(
                properties.getProperty("consumer.api.key"),
                properties.getProperty("consumer.api.secret"), 
                properties.getProperty("consumer.access.token"),
                properties.getProperty("consumer.access.token.secret"));

            return authentication;
        } catch (final Exception e) {
            System.out.println("Failed to authenticate with Twitter with exception: " + e);
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }
}