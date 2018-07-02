package org.axonframework.aws.eventhandling;

import org.axonframework.aws.SQSMessageConverter;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;
import org.axonframework.serialization.UnknownSerializedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * MessageListener implementation that deserializes incoming messages and forwards them to one or more event processors.
 * <p>
 * AWSMessageSource must be configured to listen to an incoming message from an SQS Queue through {@link org.springframework.messaging.handler.annotation.MessageMapping} and forwards each message
 * to all subscribed processors.
 * <p>
 * Note that the Processors must be subscribed before the MessageListenerContainer is started. Otherwise, messages will
 * be consumed from the SQS Queue without any processor processing them.
 * <p>
 * Implementation based on {@code org.axonframework.amqp.eventhandling.spring.SpringAMQPMessageSource}
 *
 * @author Allard Buijze (SpringAMQPMessageSource)
 * @author Rey Pader (Adaption to SQS)
 */
public class AWSMessageSource implements SubscribableMessageSource<EventMessage<?>>, MessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSMessageSource.class);

    private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();
    private final SQSMessageConverter messageConverter;

    public AWSMessageSource(SQSMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    @Override
    public Registration subscribe(Consumer<List<? extends EventMessage<?>>> messageProcessor) {
        eventProcessors.add(messageProcessor);
        return () -> eventProcessors.remove(messageProcessor);
    }

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        LOGGER.info("Handling message: {}", message);
        if (message.getPayload() instanceof byte[] || message.getPayload() instanceof String) {
            if (!eventProcessors.isEmpty()) {
                try {
                    byte[] payload;
                    if (message.getPayload() instanceof String) {
                        payload = ((String) message.getPayload()).getBytes();
                    } else {
                        payload = (byte[]) message.getPayload();
                    }
                    EventMessage<?> event = messageConverter
                            .readSQSMessage(payload, message.getHeaders()).orElse(null);
                    if (event != null) {
                        eventProcessors.forEach(ep -> ep.accept(Collections.singletonList(event)));
                    }
                } catch (UnknownSerializedTypeException e) {
                    LOGGER.warn("Unable to deserialize an incoming message. Ignoring it. {}", e.toString());
                }
            }
        } else {
            throw new IllegalArgumentException("Incoming messages should have a payload of type byte[].");
        }
    }
}
