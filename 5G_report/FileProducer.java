import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.nio.file.*;
import java.util.Properties;

public class FileProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Kafka topic to publish messages to
        String topic = "input-topic";

        // Directory path to read files from
        String directoryPath = "../test";

        // Read from the directory and its subdirectories, and publish each file's content as a message to the Kafka topic
        try {
            Files.walk(Paths.get(directoryPath)).forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try (BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            producer.send(new ProducerRecord<>(topic, line), (metadata, exception) -> {
                                if (exception != null) {
                                    exception.printStackTrace();
                                } else {

                                }
                            });
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Close the Kafka producer
        producer.close();
    }
}
