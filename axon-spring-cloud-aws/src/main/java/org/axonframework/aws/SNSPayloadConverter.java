package org.axonframework.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.cloud.aws.messaging.core.MessageAttributeDataTypes;
import org.springframework.cloud.aws.messaging.support.converter.NotificationRequestConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.NumberUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Based on {@link NotificationRequestConverter}
 *
 * @author Rey Pader
 */
public class SNSPayloadConverter {
    private final ObjectMapper jsonMapper;


    public SNSPayloadConverter() {
        this(new ObjectMapper());
    }

    public SNSPayloadConverter(ObjectMapper objectMapper) {
        this.jsonMapper = new ObjectMapper();
    }

    public Message<?> convert(String payload) {
        Assert.notNull(payload, "payload must not be null");

        JsonNode jsonNode;
        try {
            jsonNode = this.jsonMapper.readTree(payload);
        } catch (Exception e) {
            throw new MessageConversionException("Could not read JSON", e);
        }
        if (!jsonNode.has("Type")) {
            throw new MessageConversionException("Payload: '" + payload + "' does not contain a Type attribute", null);
        }

        if (!"Notification".equals(jsonNode.get("Type").asText())) {
            throw new MessageConversionException("Payload: '" + payload + "' is not a valid notification", null);
        }

        if (!jsonNode.has("Message")) {
            throw new MessageConversionException("Payload: '" + payload + "' does not contain a message", null);
        }

        String messagePayload = jsonNode.get("Message").asText();
        return new GenericMessage<>(messagePayload,
                getMessageAttributesAsMessageHeaders(jsonNode.path("MessageAttributes")));
    }

    private static Map<String, Object> getMessageAttributesAsMessageHeaders(JsonNode message) {
        Map<String, Object> messageHeaders = new HashMap<>();
        Iterator<String> fieldNames = message.fieldNames();
        while (fieldNames.hasNext()) {
            String attributeName = fieldNames.next();
            String attributeValue = message.get(attributeName).get("Value").asText();
            String attributeType = message.get(attributeName).get("Type").asText();
            if (MessageHeaders.CONTENT_TYPE.equals(attributeName)) {
                messageHeaders.put(MessageHeaders.CONTENT_TYPE, MimeType.valueOf(attributeValue));
            } else if (MessageHeaders.ID.equals(attributeName)) {
                messageHeaders.put(MessageHeaders.ID, UUID.fromString(attributeValue));
            } else {
                if (MessageAttributeDataTypes.STRING.equals(attributeType)) {
                    messageHeaders.put(attributeName, attributeValue);
                } else if (attributeType.startsWith(MessageAttributeDataTypes.NUMBER)) {
                    Object numberValue = getNumberValue(attributeType, attributeValue);
                    if (numberValue != null) {
                        messageHeaders.put(attributeName, numberValue);
                    }
                } else if (MessageAttributeDataTypes.BINARY.equals(attributeName)) {
                    messageHeaders.put(attributeName, ByteBuffer.wrap(attributeType.getBytes()));
                }
            }
        }

        return messageHeaders;
    }

    private static Object getNumberValue(String attributeType, String attributeValue) {
        String numberType = attributeType.substring(MessageAttributeDataTypes.NUMBER.length() + 1);
        try {
            Class<? extends Number> numberTypeClass = Class.forName(numberType).asSubclass(Number.class);
            return NumberUtils.parseNumber(attributeValue, numberTypeClass);
        } catch (ClassNotFoundException e) {
            throw new MessagingException(String.format("Message attribute with value '%s' and data type '%s' could not be converted " +
                    "into a Number because target class was not found.", attributeValue, attributeType), e);
        }
    }
}
