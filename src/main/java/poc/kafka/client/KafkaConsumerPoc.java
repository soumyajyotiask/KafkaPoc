package poc.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.common.collect.Lists;

public class KafkaConsumerPoc {

	private Consumer<Long, String> createConsumer() {
		Properties props = new Properties();

		props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerPoc.class.getName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Consumer<Long, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Lists.newArrayList(Constants.TOPIC));

		return consumer;
	}

	private final Consumer<Long, String> consumer = createConsumer();
	private long pollTime = 100;

	public void runConsumer() throws Exception {
		boolean gotKillPill = false;
		while (!gotKillPill) {
			ConsumerRecords<Long, String> records = consumer.poll(pollTime);
			if (records.count() != 0) {
				for (ConsumerRecord<Long, String> record : records) {
					String bookName = record.value();
					System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), bookName, record.partition(),
							record.offset());
					gotKillPill = bookName.startsWith(Constants.KILL_PILL);
					consumer.commitSync();
				}
			}
		}
		System.out.println("Done - Consuming");
		consumer.unsubscribe();
	}

	public void destroy() {
		consumer.close();
	}

	public static void main(String[] args) throws Exception {
		KafkaConsumerPoc producer = new KafkaConsumerPoc();
		producer.runConsumer();
		producer.destroy();
	}
}
