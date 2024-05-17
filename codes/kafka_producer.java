import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class WeatherDataProducer {
    public static void main(String[] args) {
        String[] cities = {"SanFrancisco", "LosAngeles", "Miami"};
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        try {
            while (true) {
                for (String city : cities) {
                    String data = String.format("%s, %dF", city, (random.nextInt(40) + 60));
                    producer.send(new ProducerRecord<>("weather-data", city, data));
                    System.out.println("Sent data: " + data);
                }
                Thread.sleep(3000); // Send data every 3 seconds
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
