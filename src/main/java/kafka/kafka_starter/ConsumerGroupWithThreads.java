package kafka.kafka_starter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroupWithThreads {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupWithThreads.class);

	public static void main(String[] args) {
		new ConsumerGroupWithThreads().run();
	}

	private ConsumerGroupWithThreads() {

	}

	private void run() {
		String bootstrapServers = "localhost:9092";
		String groupId = "my-first-id";
		String offsetConfig = "earliest";
		String topic = "programming4_topic";
		CountDownLatch latch = new CountDownLatch(1);

		LOGGER.info("Creating Consumer thread");
		Runnable consumerThread = new ConsumerRunnable(bootstrapServers, groupId, offsetConfig, topic, latch);

		// start the thread
		Thread myThread = new Thread(consumerThread);
		myThread.start();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Caught shutdown hook");
			((ConsumerRunnable) consumerThread).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOGGER.info("Application has exited");
		}));

		try {
			latch.wait();
		} catch (InterruptedException e) {
			LOGGER.info("Application got interrupted");
		} finally {
			LOGGER.info("Application is closing");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String bootstrapServers, String groupId, String offsetConfig, String topic,
				CountDownLatch latch) {
			this.latch = latch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {

			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

					for (ConsumerRecord<String, String> record : records) {
						String key = record.key();
						String value = record.value();
						int partition = record.partition();
						long offset = record.offset();
						LOGGER.info("{} {} {} {}", key, value, partition, offset);
					}
				}
			} catch (WakeupException e) {
				LOGGER.info("Recieved Shutdown signal");
			} finally {
				consumer.close();
				// tell main code we are done with consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			// is s special method to interrupt consumer.poll
			// it will throw WakeupException
			consumer.wakeup();
		}
	}
}
