package kafka.kafka_starter;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("rawtypes")
public class CustomSerializer implements Serializer {

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomSerializer.class);

	@Override
	public void configure(Map configs, boolean isKey) {
		LOGGER.info("Invoked configure");
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return retVal;
	}

	@Override
	public void close() {
		LOGGER.info("Invoked close");
	}
}
