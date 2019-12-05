package kafka.kafka_starter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerObject {

	private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		String bootstrapServers = "localhost:9092";
		String groupId = "my-first-id";
		String offsetConfig = "earliest";
		String topic = "programming4_topic";

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

		// create consumer
		@SuppressWarnings("resource")
		KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(properties,
				new CustomDeserializer<>(String.class), new CustomDeserializer<>(Student.class));

		// subscriber to topic
		consumer.subscribe(Arrays.asList(topic));

		// poll for new data
		while (true) {
			ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(200));

			for (ConsumerRecord<String, Student> record : records) {
				String key = record.key();
				Student value = record.value();
				LOGGER.info("{} {} {} {}", key, value.getName(), value.getClazz(), value.getRollno());
			}
		}

	}
}
