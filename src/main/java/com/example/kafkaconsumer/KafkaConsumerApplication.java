package com.example.kafkaconsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;

@EnableKafka
@SpringBootApplication
public class KafkaConsumerApplication {// implements CommandLineRunner {
//	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();

//		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

//		String jaasCfg = String.format(jaasTemplate, "songLoggingConsumer", "songLoggingConsumer");
//	    String jaasCfg = String.format(jaasTemplate, "test", "test");
//	    String jaasCfg = String.format(jaasTemplate, "test1", "test1");
	    String jaasCfg = String.format(jaasTemplate, "hsreco", "hsreco");
//		String jaasCfg = String.format(jaasTemplate, "svdView", "svdView");

		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "SCRAM-SHA-256");
//		props.put("security.protocol", "SASL_PLAINTEXT");
//		props.put("sasl.mechanism", "PLAIN");
		props.put("sasl.jaas.config", jaasCfg);
//		props.put("bootstrap.servers", "172.26.212.67:9091,172.26.11.100:9091,172.26.11.137:9091");
//		props.put("bootstrap.servers", "localhost:9091");
		props.put("bootstrap.servers", "172.26.69.155:9091");
		props.put("auto.offset.reset", "earliest");
		props.put("enable.auto.commit", "false");
//		props.put("enable.auto.commit", "true");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new DefaultKafkaConsumerFactory<>(props);
	}

//	 when auto.commit true
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> klf() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

	// when auto.commit true
//	@KafkaListener(topics = "email_push", groupId = "email_push", containerFactory = "klf")
	@KafkaListener(topics = "email_push", groupId = "pushservice", containerFactory = "klf")
	public void listen2(String message) {
		System.out.println("email_push mssg : " + message);

	}

	// when auto.commit false
	@Bean("kafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> manualAutoCommit() {

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		ContainerProperties containerProperties = factory.getContainerProperties();
		containerProperties.setAckMode(AckMode.MANUAL);
		return factory;
	}

//	@KafkaListener(topics = "test", groupId = "test")
//	@KafkaListener(topics = "test", groupId = "test1")

//	@KafkaListener(topics = "like_follow", groupId = "hsreco")
//	@KafkaListener(topics = "svd_view", groupId = "svdView")
//	@KafkaListener(topics = "email_push", groupId = "email_push")
// when auto.commit false
	public void listen(String message, Acknowledgment acknowledgment) {
		System.out.println("Received Messasge in group foo: " + message);
		acknowledgment.acknowledge();
	}

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(KafkaConsumerApplication.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.setLogStartupInfo(false);
		app.run(args);

	}

//	@Override
	public void run(String... args) throws Exception {
		Properties props = new Properties();
		String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, "songLoggingConsumer", "songLoggingConsumer");
//		String jaasCfg = String.format(jaasTemplate, "gdprStagReco", "gdprStagReco");
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");
		props.put("sasl.jaas.config", jaasCfg);
//		props.setProperty("bootstrap.servers", "localhost:9091");
		props.setProperty("bootstrap.servers", "172.26.212.67:9091,172.26.11.100:9091,172.26.11.137:9091");
		props.setProperty("auto.offset.reset", "earliest");
		props.setProperty("group.id", "inAppropriate");
//		props.setProperty("group.id", "reco");
		props.setProperty("enable.auto.commit", "false");
//		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("max.poll.records", 1);
//		props.setProperty("max.partition.fetch.bytes", "100");
//		props.setProperty("fetch.max.bytes", "100");
//		props.setProperty("max.poll.interval.ms", "5000");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//		consumer.subscribe(Arrays.asList("gdpr"));
		consumer.subscribe(Arrays.asList("influencer_svd"));

		/*
		 * TopicPartition topicPartition = new TopicPartition("gdpr", 0);
		 * List<TopicPartition> topics = Arrays.asList(topicPartition);
		 * consumer.assign(topics); consumer.seek(topicPartition, 0);
		 */
//		consume(consumer);
//		consume1(consumer);

	}

	public void consume(KafkaConsumer<String, String> consumer) {
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {

					System.out.println(record.value());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

	public void consume1(KafkaConsumer<String, String> consumer) {
		try {
			boolean flag = false;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				System.out.println("=========================");
				for (ConsumerRecord<String, String> record : records) {
					if (record.offset() != 21) {
						System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
								record.offset(), record.key(), record.value());
						TopicPartition partition = new TopicPartition("gdpr", record.partition());
						System.out.println("position : " + consumer.position(partition));

						Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(partition,
								new OffsetAndMetadata(record.offset() + 1));

						/*
						 * Map<TopicPartition, OffsetAndMetadata> map =
						 * Collections.singletonMap(partition, new OffsetAndMetadata(0));
						 * consumer.commitSync(map);
						 */
//						flag =true;
					}
					if (record.offset() == 51) {
						System.out.printf("inside else...........");

						Map<TopicPartition, OffsetAndMetadata> map = Collections
								.singletonMap(new TopicPartition("gdpr", 0), new OffsetAndMetadata(2 + 1));
						// consumer.commitSync(map);
					}
				}
				TimeUnit.SECONDS.sleep(3);
				if (flag)
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

}
