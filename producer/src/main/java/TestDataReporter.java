import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TestDataReporter implements Runnable {

    private static final int NUM_MESSAGES = 100;
    private final String TOPIC;

    // private Producer<Long, String> producer;
    private Producer<Long, Long> producer;

    // public TestDataReporter(final Producer<Long, String> producer, String TOPIC) {
    public TestDataReporter(final Producer<Long, Long> producer, String TOPIC) {
        this.producer = producer;
        this.TOPIC = TOPIC;
    }

    @Override
    public void run() {
        for (int i = 0; i < NUM_MESSAGES; i++) {                
            long time = System.currentTimeMillis();
			long value = new Long(i);
            System.out.println("Test Data #" + i + " from thread #" + Thread.currentThread().getId());
            
            // final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, time, "Test Data #" + i);
            final ProducerRecord<Long, Long> record = new ProducerRecord<Long, Long>(TOPIC, time, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        System.out.println(e);
                        System.exit(1);
                    }
                }
            });
        }
        System.out.println("Finished sending " + NUM_MESSAGES + " messages from thread #" + Thread.currentThread().getId() + "!");
    }
}
