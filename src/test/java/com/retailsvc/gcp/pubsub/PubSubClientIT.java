package com.retailsvc.gcp.pubsub;

import static com.retailsvc.gcp.pubsub.EmulatorRedirect.PUBSUB_EMULATOR_HOST;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class PubSubClientIT {

  String image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest";
  DockerImageName dockerImage = DockerImageName.parse(image);
  @Container PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(dockerImage);

  String testTopic = "test-topic";
  PubSubClientFactory factory;

  @BeforeEach
  void setUp() throws Exception {
    factory = createFactory();

    emulator.start();

    System.setProperty(PUBSUB_EMULATOR_HOST, emulator.getEmulatorEndpoint());

    ManagedChannel channel =
        ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

    createTopic(channelProvider, NoCredentialsProvider.create());

    channel.shutdown();
  }

  @Test
  void canPublish() {
    try (var pubSubClient = getClient()) {
      assertThatNoException().isThrownBy(() -> pubSubClient.publish("test", Map.of()));
      assertThatNoException().isThrownBy(() -> pubSubClient.publish(List.of(1, 2, 3), Map.of()));
    }
  }

  @Test
  void canPublishOrdered() {
    final var clientConfig = new PubSubClientConfig().setMessageOrderingEnabled(true);
    final var clientFactory = createFactory().setClientConfig(clientConfig);

    try (var pubSubClient = clientFactory.create(testTopic)) {
      assertThatNoException()
          .isThrownBy(() -> pubSubClient.publishOrdered("test", Map.of(), "key"));
      assertThatNoException()
          .isThrownBy(() -> pubSubClient.publishOrdered(List.of(1, 2, 3), Map.of(), "key"));
      assertThatNoException()
          .isThrownBy(() -> pubSubClient.publishOrdered(List.of(1, 2, 3), Map.of(), null));
    }
  }

  @Test
  void testClosingClients() {
    try (var pubSubClient = getClient()) {
      assertThatNoException().isThrownBy(() -> pubSubClient.publish("test", Map.of()));
    }
    try (var pubSubClient = getClient()) {
      assertThatNoException().isThrownBy(() -> pubSubClient.publish("test", Map.of()));
    }
  }

  @Test
  void testPublishAfterClosedClientThrows() {
    var pubSubClient = getClient();

    pubSubClient.close();

    assertThatException()
        .isThrownBy(() -> pubSubClient.publish("test", null))
        .isInstanceOf(PubSubClientException.class)
        .withMessage("Client is closed");
  }

  private static PubSubClientFactory createFactory() {
    return new PubSubClientFactory(
        (ObjectToBytesMapper)
            v -> ByteBuffer.wrap(String.valueOf(v).getBytes(StandardCharsets.UTF_8)));
  }

  private PubSubClient getClient() {
    return factory.create(testTopic);
  }

  private void createTopic(
      TransportChannelProvider channelProvider, NoCredentialsProvider credentialsProvider)
      throws IOException {
    TopicAdminSettings topicAdminSettings =
        TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
      TopicName topicName = TopicName.of(PubSubClientFactory.TEST_PROJECT, testTopic);
      topicAdminClient.createTopic(topicName);
    }
  }

  @AfterEach
  void tearDown() {
    emulator.stop();
  }
}
