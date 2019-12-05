package kafka.kafka_starter;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerWithCallback.class);

	public static void main(String[] args) {
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {

			String key = "id_" + Integer.toString(i);
			String topic = "programming4_topic";
			String value = "value_" + Integer.toString(i);

			// create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			LOGGER.info("Key: {}", key);

			// send data- Asynchronous
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executes when a record is successfully sent or exception
					if (exception == null) {
						LOGGER.info("Recieved new Metadata." + "\n" + "Topic:" + metadata.topic() + "\n" + "Partition:"
								+ metadata.partition() + "\n" + "Offset:" + metadata.offset() + "\n" + "Timestamp:"
								+ metadata.timestamp());
					} else {
						LOGGER.error("error while producing");
					}
				}
			});
		}
		// flush data
		producer.flush();

		// close the producer
		producer.close();
	}
}
