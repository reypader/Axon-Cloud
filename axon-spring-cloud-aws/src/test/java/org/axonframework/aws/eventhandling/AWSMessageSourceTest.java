package org.axonframework.aws.eventhandling;

import org.axonframework.aws.DefaultSQSMessageConverter;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 * @author Nakul Mishra
 * @author Rey Pader (Adaption to SQS)
 */
public class AWSMessageSourceTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testMessageListenerInvokesAllEventProcessors() throws Exception {
        Serializer serializer = new XStreamSerializer();
        Consumer<List<? extends EventMessage<?>>> eventProcessor = mock(Consumer.class);
        DefaultSQSMessageConverter messageConverter = new DefaultSQSMessageConverter(serializer);
        AWSMessageSource testSubject = new AWSMessageSource(messageConverter);
        testSubject.subscribe(eventProcessor);

        Message<byte[]> message = messageConverter.createSQSMessage(GenericEventMessage.asEventMessage("test"));

        testSubject.handleMessage(MessageBuilder.withPayload(message.getPayload()).copyHeaders(message.getHeaders()).build());

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(eventProcessor).accept(captor.capture());
        List item = captor.getValue();
        assertEquals(1, item.size());
        assertEquals("test", ((EventMessage) item.get(0)).getPayload());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMessageListenerIgnoredOnDeserializationFailure() throws Exception {
        Serializer serializer = new XStreamSerializer();
        Consumer<List<? extends EventMessage<?>>> eventProcessor = mock(Consumer.class);
        DefaultSQSMessageConverter messageConverter = new DefaultSQSMessageConverter(serializer);
        AWSMessageSource testSubject = new AWSMessageSource(messageConverter);
        testSubject.subscribe(eventProcessor);

        Message<byte[]> message = messageConverter.createSQSMessage(GenericEventMessage.asEventMessage("test"));


        Map<String, Object> h = new HashMap<>(message.getHeaders());
        h.put("axon-message-type", "strong");
        MessageHeaders headers = new MessageHeaders(h);
        testSubject.handleMessage(MessageBuilder.withPayload(message.getPayload()).copyHeaders(headers).build());

        verify(eventProcessor, never()).accept(any(List.class));
    }
}
