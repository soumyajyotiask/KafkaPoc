package poc.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import poc.kafka.client.Constants;

public class KafkaProducerPoc {


	private Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaProducerPoc.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	private final Producer<Long, String> producer = createProducer();

	public void runProducer(final int sendMessageCount) throws Exception {
		long time = System.currentTimeMillis();

		try {
			ProducerRecord<Long, String> record = null;
			for (long index = time; index < time + sendMessageCount; index++) {
				record = new ProducerRecord<>(Constants.TOPIC, index, "bOOk-" + index);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);

			}
			record = new ProducerRecord<>(Constants.TOPIC, 10000l, Constants.KILL_PILL+ "bOOk");
			RecordMetadata metadata = producer.send(record).get();
			System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) \n",
					record.key(), record.value(), metadata.partition(), metadata.offset());
			
		} finally {
			producer.flush();
		}
	}
	
	public void destroy() {
		producer.flush();
		producer.close();
	}
	
	public static void main(String[] args) throws Exception {
		KafkaProducerPoc producer = new KafkaProducerPoc();
		producer.runProducer(10);
		producer.destroy();
	}
}
