package ir.metrix.interview;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

@EnableKafka
@SpringBootApplication
public class InterviewApplication {

	public static void main(String[] args) {
		SpringApplication.run(InterviewApplication.class, args);
	}

	@Value("${topic.name}")
	private String topicName;
	@Value("${message.count}")
	private int messageCount;

	@Bean
	public NewTopic topic() { //TODO: Adjust topic configuration like partition and replica
		return TopicBuilder
				.name(topicName)
				.replicas(3)
				.partitions(2)
				.build();
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, Message> template) {
		return args -> {
			int count = messageCount;
			for (int i = 0; i < count; i++)  //we have only 2 users
				template.send(topicName, (i%2), String.valueOf(i), new Message("user"+(i%2), "just a message!"));
		};
	}

}
