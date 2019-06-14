package custom.processor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Properties;

interface MessageBinding {

	final String REQUESTS_IN = "requests-in";

	@Input(REQUESTS_IN)
	KStream<String, String> requestsIn();

}

@EnableBinding(MessageBinding.class)
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Value("${input-topic}")
	private String inputTopic;

	@Value("${broker-url}:${broker-port}")
	private String brokerUrl;

	@Bean
	public CommandLineRunner runner() {

		return args -> {
			Properties producerProperties = new Properties();
			producerProperties.put("bootstrap.servers", brokerUrl);
			producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
			String[] payloads = new String[] {"aaabbb", "bbbccc", "bbbccc", "cccaaa"};
			for (String payload: payloads) {
				String key = String.valueOf(System.currentTimeMillis());
				producer.send(new ProducerRecord<>(inputTopic, key, payload));
			}
		};
	}

	@Component
	public static class KafkaStreamConsumer {

		private Logger logger = LoggerFactory.getLogger(getClass());

		@Autowired
		private ApplicationContext applicationContext;

		private String stateStoreName = "counterKeyValueStore";

		private Long cap = 7L;

		public void initializeStateStores() throws Exception {
			StreamsBuilderFactoryBean streamsBuilderFactoryBean =
					applicationContext.getBean("&stream-builder-requestListener", StreamsBuilderFactoryBean.class);
			StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
			StoreBuilder<KeyValueStore<String, Long>> keyValueStoreBuilder =
					Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName), Serdes.String(), Serdes.Long());
			streamsBuilder.addStateStore(keyValueStoreBuilder);
		}

		@StreamListener
		public void requestListener(@Input(MessageBinding.REQUESTS_IN) KStream<String, String> requestsIn) {
			try {
				initializeStateStores();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			requestsIn
					.transform(() -> new CustomProcessor(stateStoreName, cap), stateStoreName)
					.foreach((k,v) -> {
				logger.info(k + ":" + v);
			});
		}
	}

	public static class CustomProcessor implements Transformer<String, String, KeyValue<String, Long>> {

		private Long cap;

		private String stateStoreName;

		private ProcessorContext context;

		private KeyValueStore<String, Long> kvStore;

		public CustomProcessor(String stateStoreName, Long cap) {
			this.stateStoreName = stateStoreName;
			this.cap = cap;
		}

		@Override
		public void init(ProcessorContext context) {
			this.context = context;
			this.kvStore = (KeyValueStore<String, Long>) context.getStateStore(stateStoreName);
		}

		private void findAndFlushCandidates() {
			Iterator<KeyValue<String, Long>> it = kvStore.all();
			while(it.hasNext()) {
				KeyValue<String, Long> entry = it.next();
				if (entry.value >= cap) {
					this.context.forward(entry.key, entry.value);
					kvStore.delete(entry.key);
				}
			}
		}

		@Override
		public KeyValue<String, Long> transform(String key, String value) {
			for (String c : value.split("")) {
				Long counter = kvStore.get(c);
				if (counter!= null) {
					kvStore.put(c, counter + 1);
					continue;
				}
				kvStore.put(c, 1L);
			}
			findAndFlushCandidates();
			return null;
		}

		@Override
		public void close() {
			// Perform some cleanup
		}
	}

}