package kafka.kafka_starter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignAndSeek {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroups.class);

	public static void main(String[] args) {

		String bootstrapServers = "localhost:9092";
		String offsetConfig = "earliest";
		String topic = "programming4_topic";

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

		// create consumer
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// assign and seek are used to replay date or fetch specific messge
		TopicPartition partitionToRead = new TopicPartition(topic, 0);
		long offsetToRead = 20L;
		consumer.assign(Arrays.asList(partitionToRead));

		// seek
		consumer.seek(partitionToRead, offsetToRead);

		int numberOfMessagesToread = 5;
		boolean keepOnReading = true;
		int numberOfMessagesRead = 0;

		// poll for new data
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

			for (ConsumerRecord<String, String> record : records) {
				numberOfMessagesRead += 1;
				String key = record.key();
				String value = record.value();
				int partition = record.partition();
				long offset = record.offset();
				LOGGER.info("KEY: {} VALUE: {} PARTITION:{} OFFSET:{}", key, value, partition, offset);
				if (numberOfMessagesRead >= numberOfMessagesToread) {
					keepOnReading = false;
					break;
				}
			}
		}
		LOGGER.info("Exiting the application");
	}
}
