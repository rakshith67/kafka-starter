package kafka.kafka_starter;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("rawtypes")
public class CustomDeserializer<T> implements Deserializer {

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomSerializer.class);

	private Class<T> type;

	@SuppressWarnings("unchecked")
	public CustomDeserializer(Class type) {
		this.type = type;
	}

	public CustomDeserializer() {
	}

	@Override
	public void configure(Map map, boolean b) {
		LOGGER.info("Invoked configure");
	}

	@Override
	public T deserialize(String s, byte[] bytes) {
		ObjectMapper mapper = new ObjectMapper();
		T obj = null;
		try {
			obj = mapper.readValue(bytes, type);
		} catch (Exception e) {

			LOGGER.error(e.getMessage());
		}
		return obj;
	}

	@Override
	public void close() {
		LOGGER.info("Invoked close");
	}
}
