package ir.metrix.interview;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumer.class.getName());

    private final String metricsMessage = """
            
            ____________________________________________
            USER {} Consumer:
            Batch processed message count is {}.
            Process execute in {} seconds.
            Speed is {} message per seconds.
            ____________________________________________
            """;
    private final MessageProcessorService service;

    MessageConsumer(MessageProcessorService service) {
        this.service = service;
    }

    @KafkaListener(id = "user1Listener", topicPartitions =
            { @TopicPartition(topic = "${topic.name}", partitions = "0")},
            batch = "true"
    ) // by using partitioning can consume specific user messages, this consumer just consume user-1 messages.
    public void consumeUser1(List<Message> records) {
        long startTime = System.currentTimeMillis();
        service.processMultipleMessage(records); // with batch poll, processes multiple messages instead of one by one, to decrease network latency.
        long deltaSecond = (System.currentTimeMillis() - startTime) / 1000; // time to process batch messages in seconds.
        long messagePerSecondSpeed = records.size() / deltaSecond; // speed of this consumer, message per seconds.
        LOG.info(metricsMessage, "1", records.size(), deltaSecond, messagePerSecondSpeed);
    }

    @KafkaListener(id = "user2Listener", topicPartitions =
            { @TopicPartition(topic = "${topic.name}", partitions = "1")},
            batch = "true"
    ) // this consumer just consume user 2 messages.
    public void consumeUser2(List<Message> records) {
        long startTime = System.currentTimeMillis();
        service.processMultipleMessage(records);
        long deltaSecond = (System.currentTimeMillis() - startTime) / 1000;
        long messagePerSecondSpeed = records.size() / deltaSecond;
        LOG.info(metricsMessage, "2", records.size(), deltaSecond, messagePerSecondSpeed);
    }
}
