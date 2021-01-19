package com.example.demo;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
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
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
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
    Subscriber s = Subscriber.newBuilder(subscriptionName, ms)
        .setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(500L)
            .build())
        .build();
    s.startAsync().awaitRunning();

    Publisher p  = Publisher.newBuilder(fullTopic)
        .setBatchingSettings(BatchingSettings.newBuilder()
            .setElementCountThreshold(1000L)
            .build())
        .build();

    Flux.range(0, 1000)
        .log()
        .subscribe(j ->
            p.publish(PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8("message num: " + j))
                .build())
        );

    ms.getMessages()
        .map(m -> {
          m.ack();
          return new String(m.getData().toByteArray(), StandardCharsets.UTF_8);
        })
        .log()
        .subscribe();
  }

  @Component
  public static class MySubscriber implements MessageReceiver {

    private Consumer<AckablePubsubMessage> messageConsumer;

    public Flux<AckablePubsubMessage> getMessages() {
      return Flux.create(sink -> MySubscriber.this.messageConsumer = sink::next);
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer acker) {
      messageConsumer.accept(new AckablePubsubMessage(message, acker));
    }
  }

  @RequiredArgsConstructor
  public static class AckablePubsubMessage implements AckReplyConsumer {
    @Delegate
    private final PubsubMessage pubsubMessage;
    @Delegate
    private final AckReplyConsumer acker;
  }
}
