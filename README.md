# ehub-twitter

A "Hello, Twitter" application which utilizes Kafka Producer, Consumer, and Streams APIs 
for educational purposes.

# Dependencies

* Java JDK 1.8+
* Maven
* Kafka-compliant cluster for the Producer and Consumer
* A real Kafka cluster for the Streams API application (`TwitterWordCount.java`)

# Usage

## Configuration

Make two `resources` directories: one in `consumer/src/main/` and one in 
`producer/src/main/`. Add a `<producer|consumer>.config` file in each directory, 
respectively, of the following format:

```
bootstrap.servers={{ MY_KAFKA_CLUSTER }}:9092
ssl.endpoint.identification.algorithm=https
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="{{ USERNAME }}" password="{{ PASSWORD }}";
```

Be sure to create topics of your choice and input their names in the source files.

## Producer

```
cd producer/src/main/java
mvn clean package
mvn exec:java -Dexec.mainClass=TwitterProducer -Dexec.args="keywords and #hashtags to search for"
```

## Consumer

```
cd consumer/src/main/java
mvn clean package
mvn exec:java -Dexec.mainClass=TwitterConsumer
```

The consumer will print tweets to STDOUT in near real-time.

## Word count

```
cd producer/src/main/java
mvn exec:java -Dexec.mainClass=TwitterWordCount
```

Make sure an appropriate `OUTPUT_TOPIC` is created and configured before execution.