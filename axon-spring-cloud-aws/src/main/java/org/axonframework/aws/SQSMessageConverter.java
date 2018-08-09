package org.axonframework.aws;

import org.axonframework.eventhandling.EventMessage;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Optional;

public interface SQSMessageConverter {
    Message<byte[]> createSQSMessage(EventMessage<?> eventMessage);

    Optional<EventMessage<?>> readSQSMessage(byte[] messageBody, MessageHeaders headers);
}
