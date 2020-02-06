package com.example.kafkaconsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import ch.qos.logback.classic.Logger;

@SpringBootApplication
public class KafkaConsumerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(KafkaConsumerApplication.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.setLogStartupInfo(false);
		app.run(args);

	}

	@Override
	public void run(String... args) throws Exception {
		Properties props = new Properties();
		String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, "gdprAdmin", "gdprAdmin");
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");
		props.put("sasl.jaas.config", jaasCfg);
		props.setProperty("bootstrap.servers", "localhost:9091");
		props.setProperty("group.id", "payment");
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("max.poll.records", 1);
//		props.setProperty("max.partition.fetch.bytes", "1");
//		props.setProperty("fetch.max.bytes", "1");
		props.setProperty("max.poll.interval.ms", "5000");
//		props.setProperty("auto.offset.reset", "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("gdpr"));

		/*
		 * TopicPartition topicPartition = new TopicPartition("gdpr", 0);
		 * List<TopicPartition> topics = Arrays.asList(topicPartition);
		 * consumer.assign(topics); consumer.seek(topicPartition, 0);
		 */

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
						System.out.println("position : "+consumer.position(partition));

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
