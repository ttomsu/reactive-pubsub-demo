package com.example.demo;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
@Configuration
public class DemoApplication {

  public static void main(String[] args) throws IOException {
    ReactorDebugAgent.init();
    SpringApplication.run(DemoApplication.class, args);

    final String project = "ttomsu-dev-spinnaker";
    final String topicName = "flux-demo";
    final String fullTopic = ProjectTopicName.format(project, topicName);
    final String subscriptionName = ProjectSubscriptionName.format(project, topicName + "-subscription");

    MySubscriber ms = new MySubscriber();
    Subscriber.newBuilder(subscriptionName, ms)
        .setCredentialsProvider(GoogleCredentials::getApplicationDefault)
        .setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(10L)
            .build())
        .build().startAsync().awaitRunning();

    Publisher p  = Publisher.newBuilder(fullTopic)
        .setCredentialsProvider(GoogleCredentials::getApplicationDefault)
        .setBatchingSettings(BatchingSettings.newBuilder()
            .setElementCountThreshold(5L)
            .build())
        .build();

    ms.goFlux()
        .map(m -> new String(m.getData().toByteArray(), StandardCharsets.UTF_8))
        .log()
        .subscribe();

    AtomicInteger i = new AtomicInteger(0);
    Flux.range(0, 30)
        .log()
        .subscribe(j -> {
            p.publish(PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("message num: " + j))
              .build());
        });

  }

  @Component
  public static class MySubscriber implements MessageReceiver {

    private Consumer<PubsubMessage> messageConsumer;

    public Flux<PubsubMessage> goFlux() {
      return Flux.create(sink -> MySubscriber.this.messageConsumer = sink::next);
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      consumer.ack();
      messageConsumer.accept(message);
    }
  }
}
