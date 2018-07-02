package org.axonframework.aws.autoconfigure;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import org.axonframework.aws.eventhandling.AWSPublisher;
import org.axonframework.aws.DefaultSQSMessageConverter;
import org.axonframework.aws.SQSMessageConverter;
import org.axonframework.boot.autoconfig.AxonAutoConfiguration;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.aws.messaging.core.NotificationMessagingTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(NotificationMessagingTemplate.class)
@EnableConfigurationProperties(AWSPProperties.class)
@AutoConfigureAfter({AxonAutoConfiguration.class})
public class AxonAWSAutoConfiguration {

    private Logger LOGGER = LoggerFactory.getLogger(AxonAWSAutoConfiguration.class);

    @ConditionalOnMissingBean
    @Bean
    public SQSMessageConverter sqsMessageConverter(@Qualifier("eventSerializer") Serializer eventSerializer) {
        LOGGER.info("Initializing DefaultSQSMessageConverter. Using serializer {}", eventSerializer.getClass().getCanonicalName());
        return new DefaultSQSMessageConverter(eventSerializer);
    }

    @ConditionalOnExpression("!${axon.aws.publish-to-queue:false}")
    @ConditionalOnMissingBean
    @Bean(initMethod = "start", destroyMethod = "shutDown")
    public AWSPublisher snsBridge(EventBus eventBus, AmazonSNS amazonSns, SQSMessageConverter sqsMessageConverter, AWSPProperties amqpProperties) {
        LOGGER.info("Initializing AWSPublisher for SNS");
        return new AWSPublisher(eventBus, amazonSns, sqsMessageConverter, amqpProperties.getSnsTopicName());
    }

    @ConditionalOnProperty("axon.aws.publish-to-queue")
    @ConditionalOnMissingBean
    @Bean(initMethod = "start", destroyMethod = "shutDown")
    public AWSPublisher sqsBridge(EventBus eventBus, AmazonSQSAsync amazonSqs, SQSMessageConverter sqsMessageConverter, AWSPProperties amqpProperties) {
        LOGGER.info("Initializing AWSPublisher for SQS");
        return new AWSPublisher(eventBus, amazonSqs, sqsMessageConverter, amqpProperties.getSqsQueueName());
    }


}
