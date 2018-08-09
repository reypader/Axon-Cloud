/*
 * Copyright (c) 2010-2016. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.aws.eventhandling;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import org.axonframework.aws.EventPublicationFailedException;
import org.axonframework.aws.SQSMessageConverter;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.aws.messaging.core.NotificationMessagingTemplate;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.core.support.AbstractMessageChannelMessagingSendingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import java.util.List;


/**
 * EventBusTerminal implementation that uses AWS SQS or SNS to dispatch event messages. All
 * outgoing messages are sent accordingly to the proper channel using {@code axon.aws.sqs-queue-name} or {@code axon.aws.sns-topic-name}.
 * <p>
 * This terminal does not dispatch Events internally, as it relies on each event processor to listen to it's own SQS queue.
 * <p>
 * NOTE:
 * When publishing to an SNS topic, events will not be dispatched by {@link AWSMessageSource} to processors unless those SQS queues are subscribed to the topic.
 * <p>
 * Implementation based on {@code org.axonframework.amqp.eventhandling.spring.SpringAMQPPublisher}
 *
 * @author Allard Buijze (SpringAMQPPublisher)
 * @author Rey Pader (Adaption to SQS)
 */
public class AWSPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSPublisher.class);

    private final SubscribableMessageSource<EventMessage<?>> messageSource;
    private final AbstractMessageChannelMessagingSendingTemplate messagingTemplate;

    private Registration eventBusRegistration;
    private SQSMessageConverter messageConverter;
    private String publishDestination;

    public AWSPublisher(SubscribableMessageSource<EventMessage<?>> messageSource, AmazonSNS amazonSns, SQSMessageConverter messageConverter, String publishDestination) {
        this.messageSource = messageSource;
        this.messagingTemplate = new NotificationMessagingTemplate(amazonSns);
        this.messageConverter = messageConverter;
        this.publishDestination = publishDestination;
    }

    public AWSPublisher(SubscribableMessageSource<EventMessage<?>> messageSource, AmazonSQSAsync amazonSqs, SQSMessageConverter messageConverter, String publishDestination) {
        this.messageSource = messageSource;
        this.messagingTemplate = new QueueMessagingTemplate(amazonSqs);
        this.messageConverter = messageConverter;
        this.publishDestination = publishDestination;
    }

    public void start() {
        eventBusRegistration = messageSource.subscribe(this::send);
    }

    public void shutDown() {
        if (eventBusRegistration != null) {
            eventBusRegistration.cancel();
            eventBusRegistration = null;
        }
    }

    protected void send(List<? extends EventMessage<?>> events) {
        LOGGER.info("Publishing {} events to {}", events.size(), publishDestination);
        try {
            for (EventMessage event : events) {
                Message message = messageConverter.createSQSMessage(event);
                doSendMessage(message);
            }
        } catch (MessagingException e) {
            LOGGER.error("Failed to publish message", e);
            throw new EventPublicationFailedException("Failed to dispatch Events to the Message Broker.", e);
        }
    }

    protected void doSendMessage(Message amqpMessage) {
        LOGGER.info("Publishing message: {}", amqpMessage);
        LOGGER.debug("Payload: {}", amqpMessage.getPayload());
        messagingTemplate.send(publishDestination, amqpMessage);
    }

}
