package ir.metrix.interview;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class MessageProcessorService {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessorService.class.getName());

    @Value("${message.count}")
    private int messageCount;
    private final long startTime = System.currentTimeMillis();
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final Object lock = new Object();

    private final UserService userService;
    private final SmsService smsService;

    MessageProcessorService(UserService userService, SmsService smsService) {
        this.userService = userService;
        this.smsService = smsService;
    }

    public void processMessage(Message message) {
        String phoneNumber = userService.fetchPhoneById(message.getUserId());
        smsService.sendMessage(phoneNumber, message);

        synchronized (lock) {
            if (processedCount.get() == 0) {
                LOG.info("Start Processing...");
            }
            LOG.info("____________________________________________");
            LOG.info("Processed messages count: {}, in {} seconds.", this.processedCount.incrementAndGet(),
                    (System.currentTimeMillis() - startTime)/1000);
            LOG.info("____________________________________________");
            if (processedCount.get() == messageCount) {
                LOG.info("Finished Processing {} messages in {} seconds.", messageCount,(System.currentTimeMillis() - startTime)/1000);
            }
        }
    }

    public void processMultipleMessage(List<Message> messages) {
        String phoneNumber = userService.fetchPhoneById(messages.get(0).getUserId());
        smsService.sendMessages(phoneNumber, messages);

        synchronized (lock){
            if (processedCount.get() == 0) {
                LOG.info("Start Processing ...");
            }
            LOG.info("____________________________________________");
            LOG.info("Processed messages count: {}, in {} seconds.", this.processedCount.addAndGet(messages.size()),
                    (System.currentTimeMillis() - startTime) / 1000);
            LOG.info("____________________________________________");
            if (processedCount.get() == messageCount) {
                LOG.info("Finished processing {} messages in {} seconds.", messageCount, (System.currentTimeMillis() - startTime) / 1000);
            }
        }
    }
}
