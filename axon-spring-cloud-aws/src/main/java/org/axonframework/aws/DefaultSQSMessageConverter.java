package org.axonframework.aws;

import org.axonframework.common.DateTimeUtils;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.*;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.*;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.axonframework.serialization.MessageSerializer.serializePayload;

public class DefaultSQSMessageConverter implements SQSMessageConverter {

    private final Serializer serializer;

    public DefaultSQSMessageConverter(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public Message<?> createSQSMessage(EventMessage<?> eventMessage) {
        SerializedObject<String> serializedObject = serializePayload(eventMessage, serializer, String.class);
        Map<String, Object> headers = new HashMap<>();
        eventMessage.getMetaData().forEach((k, v) -> headers.put("axon-metadata-" + k, v));
        headers.put("axon-message-id", eventMessage.getIdentifier());
        headers.put("axon-message-type", serializedObject.getType().getName());
        headers.put("axon-message-revision", serializedObject.getType().getRevision());
        headers.put("axon-message-timestamp", formatInstant(eventMessage.getTimestamp()));
        if (eventMessage instanceof DomainEventMessage) {
            headers.put("axon-message-aggregate-id", ((DomainEventMessage) eventMessage).getAggregateIdentifier());
            headers.put("axon-message-aggregate-seq", ((DomainEventMessage) eventMessage).getSequenceNumber());
            headers.put("axon-message-aggregate-type", ((DomainEventMessage) eventMessage).getType());
        }
        return MessageBuilder.withPayload(serializedObject.getData()).copyHeaders(headers).build();
    }

    @Override
    public Optional<EventMessage<?>> readSQSMessage(byte[] messageBody, MessageHeaders headers) {
        if (!headers.keySet().containsAll(Arrays.asList("axon-message-id", "axon-message-type"))) {
            return Optional.empty();
        }
        Map<String, Object> metaData = new HashMap<>();
        headers.forEach((k, v) -> {
            if (k.startsWith("axon-metadata-")) {
                metaData.put(k.substring("axon-metadata-".length()), v);
            }
        });
        SimpleSerializedObject<byte[]> serializedMessage = new SimpleSerializedObject<>(messageBody, byte[].class,
                Objects.toString(headers.get("axon-message-type")),
                Objects.toString(headers.get("axon-message-revision"), null));
        SerializedMessage<?> message = new SerializedMessage<>(Objects.toString(headers.get("axon-message-id")),
                new LazyDeserializingObject<>(serializedMessage, serializer),
                new LazyDeserializingObject<>(MetaData.from(metaData)));
        String timestamp = Objects.toString(headers.get("axon-message-timestamp"));
        if (headers.containsKey("axon-message-aggregate-id")) {
            return Optional.of(new GenericDomainEventMessage<>(Objects.toString(headers.get("axon-message-aggregate-type")),
                    Objects.toString(headers.get("axon-message-aggregate-id")),
                    (Long) headers.get("axon-message-aggregate-seq"),
                    message, () -> DateTimeUtils.parseInstant(timestamp)));
        } else {
            return Optional.of(new GenericEventMessage<>(message, () -> DateTimeUtils.parseInstant(timestamp)));
        }
    }
}
