package org.axonframework.aws.autoconfigure;


import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(
        prefix = "axon.aws"
)
public class AWSPProperties {
    private String sqsQueueName;
    private String snsTopicName;

    public String getSqsQueueName() {
        return sqsQueueName;
    }

    public void setSqsQueueName(String sqsQueueName) {
        this.sqsQueueName = sqsQueueName;
    }

    public String getSnsTopicName() {
        return snsTopicName;
    }

    public void setSnsTopicName(String snsTopicName) {
        this.snsTopicName = snsTopicName;
    }
}
