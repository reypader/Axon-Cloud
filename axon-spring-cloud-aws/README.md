Axon Spring Cloud AWS
====
Heavily based on [axon-amqp](https://github.com/AxonFramework/AxonFramework/tree/master/amqp) work done by the Axon Framework team.

This is a SpringBoot-based plugin to enable publishing and subscribing of events through AWS.
The events are published through SNS and are consumed through SQS. It works under the assumption
that the SQS queue is subscribed to the SNS topic.  


## Configuration
Define the following properties in the property file
```
cloud.aws.credentials.accessKey=AKIAIOSFODNN7EXAMPLE
cloud.aws.credentials.secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY                                

# Set to false when testing locally
cloud.aws.credentials.instanceProfile=false
cloud.aws.region.auto=false 
cloud.aws.region.static=ap-southeast-1

axon.aws.sqs-queue-name=sample-queue
axon.aws.sns-topic-name=sample-topic
axon.aws.publish-to-queue=false
```

Similar to [axon-amqp](https://github.com/AxonFramework/AxonFramework/tree/master/amqp), you would need to create a bean `AWSMessageSource`.

```java
  @ConditionalOnExpression("!${axon.aws.publish-to-queue:false}")
  @Bean
  public AWSMessageSource snsMessageSource(SQSMessageConverter messageConverter) {
      return new AWSMessageSource(messageConverter) {
          private SNSPayloadConverter payloadConverter = new SNSPayloadConverter();
 
          @MessageMapping("${axon.aws.sqs-queue-name}")
          public void receive(String payload) {
              LOGGER.info("Received message: {}", payload);
              Message<?> convert = payloadConverter.convert(payload);
              LOGGER.info("Received message payload: {}", convert.getPayload());
              this.handleMessage(convert);
          }
      };
  }
```

If you'd like to bypass publishing to SNS and push the events to an SQS queue directly, you should set `axon.aws.publish-to-queue` to true and then declare a message source similar to the one below.

```java
  @ConditionalOnProperty("axon.aws.publish-to-queue")
  @Bean
  public AWSMessageSource AWSMessageSource(SQSMessageConverter messageConverter) {
      return new AWSMessageSource(messageConverter) {
 
          @MessageMapping("${axon.aws.sqs-queue-name}")
          public void receive(@Payload String payload, @Headers Map<String,String> headers) {
              LOGGER.info("Received message: {}", payload);
              Message<?> message = MessageBuilder.withPayload(payload).copyHeaders(headers).build();
              LOGGER.info("Received message payload: {}", message.getPayload());
              this.handleMessage(message);
          }
      };
  }
```