package org.axonframework.aws;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Allard Buijze
 * @author Nakul Mishra
 * @author Rey Pader (Adaption to SQS)
 */
public class DefaultSQSMessageConverterTest {

    private DefaultSQSMessageConverter testSubject;

    @Before
    public void setUp() {
        testSubject = new DefaultSQSMessageConverter(new XStreamSerializer());
    }

    @Test
    public void testWriteAndReadEventMessage() {
        EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload").withMetaData(MetaData.with("key", "value"));
        Message<byte[]> sqsMessage = testSubject.createSQSMessage(eventMessage);
        EventMessage<?> actualResult = testSubject.readSQSMessage(sqsMessage.getPayload(), sqsMessage.getHeaders())
                .orElseThrow(() -> new AssertionError("Expected valid message"));

        assertEquals(eventMessage.getIdentifier(), sqsMessage.getHeaders().get("axon-message-id"));
        assertEquals(eventMessage.getIdentifier(), actualResult.getIdentifier());
        assertEquals(eventMessage.getMetaData(), actualResult.getMetaData());
        assertEquals(eventMessage.getPayload(), actualResult.getPayload());
        assertEquals(eventMessage.getPayloadType(), actualResult.getPayloadType());
        assertEquals(eventMessage.getTimestamp(), actualResult.getTimestamp());
    }

    @Test
    public void testMessageIgnoredIfNotAxonMessageIdPresent() {
        EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload").withMetaData(MetaData.with("key", "value"));
        Message<byte[]> sqsMessage = testSubject.createSQSMessage(eventMessage);

        Map<String,Object> h = new HashMap<>(sqsMessage.getHeaders());
        h.remove("axon-message-id");
        MessageHeaders headers = new MessageHeaders(h);
        assertFalse(testSubject.readSQSMessage(sqsMessage.getPayload(), headers).isPresent());
    }

    @Test
    public void testMessageIgnoredIfNotAxonMessageTypePresent() {
        EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload").withMetaData(MetaData.with("key", "value"));
        Message<byte[]> sqsMessage = testSubject.createSQSMessage(eventMessage);

        Map<String,Object> h = new HashMap<>(sqsMessage.getHeaders());
        h.remove("axon-message-type");
        MessageHeaders headers = new MessageHeaders(h);
        assertFalse(testSubject.readSQSMessage(sqsMessage.getPayload(), headers).isPresent());
    }

    @Test
    public void testWriteAndReadDomainEventMessage() {
        DomainEventMessage<?> eventMessage = new GenericDomainEventMessage<>("Stub", "1234", 1L, "Payload", MetaData.with("key", "value"));
        Message<byte[]> sqsMessage = testSubject.createSQSMessage(eventMessage);
        EventMessage<?> actualResult = testSubject.readSQSMessage(sqsMessage.getPayload(), sqsMessage.getHeaders())
                .orElseThrow(() -> new AssertionError("Expected valid message"));

        assertEquals(eventMessage.getIdentifier(), sqsMessage.getHeaders().get("axon-message-id"));
        assertEquals("1234", sqsMessage.getHeaders().get("axon-message-aggregate-id"));
        assertEquals(1L, sqsMessage.getHeaders().get("axon-message-aggregate-seq"));

        assertTrue(actualResult instanceof DomainEventMessage);
        assertEquals(eventMessage.getIdentifier(), actualResult.getIdentifier());
        assertEquals(eventMessage.getMetaData(), actualResult.getMetaData());
        assertEquals(eventMessage.getPayload(), actualResult.getPayload());
        assertEquals(eventMessage.getPayloadType(), actualResult.getPayloadType());
        assertEquals(eventMessage.getTimestamp(), actualResult.getTimestamp());
        assertEquals(eventMessage.getAggregateIdentifier(), ((DomainEventMessage) actualResult).getAggregateIdentifier());
        assertEquals(eventMessage.getType(), ((DomainEventMessage) actualResult).getType());
        assertEquals(eventMessage.getSequenceNumber(), ((DomainEventMessage) actualResult).getSequenceNumber());
    }
}