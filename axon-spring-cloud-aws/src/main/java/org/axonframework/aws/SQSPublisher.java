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

package org.axonframework.aws;

import com.amazonaws.services.sns.AmazonSNS;
import org.axonframework.aws.autoconfigure.AWSPProperties;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.aws.messaging.core.NotificationMessagingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import java.util.List;

public class SQSPublisher implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSPublisher.class);

    private final SubscribableMessageSource<EventMessage<?>> messageSource;
    private final NotificationMessagingTemplate messagingTemplate;

    private Registration eventBusRegistration;
    private SQSMessageConverter messageConverter;
    private AWSPProperties properties;

    public SQSPublisher(SubscribableMessageSource<EventMessage<?>> messageSource, AmazonSNS amazonSns, SQSMessageConverter messageConverter, AWSPProperties properties) {
        this.messageSource = messageSource;
        this.messagingTemplate = new NotificationMessagingTemplate(amazonSns);
        this.messageConverter = messageConverter;
        this.properties = properties;
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
        LOGGER.info("Sending {} events to SNS topic {}", events.size(), properties.getSnsTopicName());
        try {
            for (EventMessage event : events) {
                Message message = messageConverter.createSQSMessage(event);
                doSendMessage(message);
            }
        } catch (MessagingException e) {
            throw new EventPublicationFailedException("Failed to dispatch Events to the Message Broker.", e);
        }
    }

    protected void doSendMessage(Message amqpMessage) {
        LOGGER.info("Publishing message: {}", amqpMessage);
        LOGGER.debug("Payload: {}", amqpMessage.getPayload());
        messagingTemplate.send(properties.getSnsTopicName(), amqpMessage);
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}
