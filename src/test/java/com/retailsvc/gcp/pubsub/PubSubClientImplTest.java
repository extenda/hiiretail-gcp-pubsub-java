package com.retailsvc.gcp.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PubSubClientImplTest {

  @Mock ObjectToBytesMapper objectMapper;
  @Mock Publisher mockPublisher;

  @Test
  void canInstantiate() {
    assertThatNoException().isThrownBy(this::createClient);
  }

  static Stream<Arguments> publishInput() {
    var testPayload = new TestPayload("value");
    return Stream.of(
        arguments("", null),
        arguments("{\"key\":\"value\"}", null),
        arguments(testPayload, Map.of()),
        arguments(ByteBuffer.wrap("value".getBytes(UTF_8)), Map.of()),
        arguments(ByteBuffer.wrap("value".getBytes(UTF_8)).flip(), Map.of()),
        arguments(new ByteArrayInputStream("value".getBytes(UTF_8)), Map.of()),
        arguments(testPayload, Map.of("attribute-1", "value-1")));
  }

  @ParameterizedTest
  @MethodSource("publishInput")
  void testCanPublish(Object payload, Map<String, String> attributes) throws Exception {
    lenient().when(objectMapper.valueAsBytes(any())).thenReturn(ByteBuffer.wrap(new byte[0]));
    when(mockPublisher.publish(any())).thenReturn(ApiFutures.immediateFuture(""));

    try (PubSubClientImpl client = createClient()) {
      assertThatNoException().isThrownBy(() -> client.publish(payload, attributes));
    }
    verify(mockPublisher).publish(any(PubsubMessage.class));
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(10L, TimeUnit.SECONDS);
  }

  @ParameterizedTest
  @NullAndEmptySource
  void testThrowsOnMissingPayload(String payload) throws Exception {
    try (PubSubClientImpl client = createClient()) {
      assertThatException().isThrownBy(() -> client.publish(payload, null));
    }
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(10L, TimeUnit.SECONDS);
  }

  @Test
  void testThrowsExecutionException() throws Exception {
    doAnswer(
            ignored -> {
              throw new ExecutionException(new RuntimeException("FROM TEST"));
            })
        .when(mockPublisher)
        .publish(any());

    try (PubSubClientImpl client = createClient()) {
      assertThatException()
          .isThrownBy(() -> client.publish("", null))
          .isInstanceOf(PubSubClientException.class);
    }
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(10L, TimeUnit.SECONDS);
  }

  @Test
  void testThrowsOnClosedClient() {
    PubSubClientImpl client = createClient();
    client.close();

    assertThatException()
        .isThrownBy(() -> client.publish("", null))
        .isInstanceOf(PubSubClientException.class)
        .withMessage("Client is closed");
  }

  @Test
  void testThrowsTimeoutException() throws Exception {
    doAnswer(
            ignored -> {
              throw new TimeoutException("FROM TEST");
            })
        .when(mockPublisher)
        .publish(any());

    try (PubSubClientImpl client = createClient()) {
      assertThatException()
          .isThrownBy(() -> client.publish("", null))
          .isInstanceOf(PubSubClientException.class);
    }

    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(10L, TimeUnit.SECONDS);
  }

  @Test
  void testThrowsIOExceptionOnMissingMapper() {
    try (PubSubClientImpl client = new PubSubClientImpl(() -> mockPublisher, null)) {
      var payload = new Object();
      Map<String, String> attributes = Map.of();
      var ex = assertThrows(PubSubClientException.class, () -> client.publish(payload, attributes));
      assertThat(ex).hasCauseInstanceOf(IOException.class);
    }
  }

  @Test
  void publishAllReturnsIdsInOrder() {
    when(mockPublisher.publish(any()))
        .thenReturn(
            ApiFutures.immediateFuture("id-1"),
            ApiFutures.immediateFuture("id-2"),
            ApiFutures.immediateFuture("id-3"));

    try (PubSubClientImpl client = createClient()) {
      var ids =
          client.publishAll(
              List.of(
                  OutgoingMessage.of("a", null),
                  OutgoingMessage.of("b", null),
                  OutgoingMessage.of("c", null)));
      assertThat(ids).containsExactly("id-1", "id-2", "id-3");
    }
    verify(mockPublisher, times(3)).publish(any(PubsubMessage.class));
  }

  @Test
  void publishAllSubmitsEveryMessageThenThrowsOnPartialFailure() {
    when(mockPublisher.publish(any()))
        .thenReturn(
            ApiFutures.immediateFuture("id-1"),
            ApiFutures.immediateFailedFuture(new RuntimeException("boom")),
            ApiFutures.immediateFuture("id-3"));

    try (PubSubClientImpl client = createClient()) {
      assertThatException()
          .isThrownBy(
              () ->
                  client.publishAll(
                      List.of(
                          OutgoingMessage.of("a", null),
                          OutgoingMessage.of("b", null),
                          OutgoingMessage.of("c", null))))
          .isInstanceOf(PubSubClientException.class)
          .withMessageContaining("Failed to publish 1 of 3 messages");
    }
    // Every message is submitted to the publisher even though the second one fails.
    verify(mockPublisher, times(3)).publish(any(PubsubMessage.class));
  }

  @Test
  void publishAllReturnsEmptyForEmptyInput() {
    try (PubSubClientImpl client = createClient()) {
      assertThat(client.publishAll(List.of())).isEmpty();
    }
    verify(mockPublisher, never()).publish(any());
  }

  @Test
  void publishAllThrowsOnClosedClient() {
    PubSubClientImpl client = createClient();
    client.close();

    assertThatException()
        .isThrownBy(() -> client.publishAll(List.of(OutgoingMessage.of("a", null))))
        .isInstanceOf(PubSubClientException.class)
        .withMessage("Client is closed");
  }

  @Test
  void publishAllAppliesSharedAttributesAndOrderingKey() {
    when(mockPublisher.publish(any()))
        .thenReturn(ApiFutures.immediateFuture("id-1"), ApiFutures.immediateFuture("id-2"));
    var captor = ArgumentCaptor.forClass(PubsubMessage.class);

    try (PubSubClientImpl client = createClient()) {
      client.publishAll(
          List.of(
              OutgoingMessage.of("a", Map.of("Kind", "test")),
              OutgoingMessage.ordered("b", Map.of("Kind", "test"), "key-b")));
    }

    verify(mockPublisher, times(2)).publish(captor.capture());
    assertThat(captor.getAllValues())
        .allSatisfy(
            message -> assertThat(message.getAttributesMap()).containsEntry("Kind", "test"));
    assertThat(captor.getAllValues().get(0).getOrderingKey()).isEmpty();
    assertThat(captor.getAllValues().get(1).getOrderingKey()).isEqualTo("key-b");
  }

  @Test
  void publishAllConvenienceWrapperAppliesAttributesToAllPayloads() {
    when(mockPublisher.publish(any()))
        .thenReturn(ApiFutures.immediateFuture("id-1"), ApiFutures.immediateFuture("id-2"));
    var captor = ArgumentCaptor.forClass(PubsubMessage.class);

    try (PubSubClientImpl client = createClient()) {
      var ids = client.publishAll(List.of("a", "b"), Map.of("Kind", "test"));
      assertThat(ids).containsExactly("id-1", "id-2");
    }

    verify(mockPublisher, times(2)).publish(captor.capture());
    assertThat(captor.getAllValues())
        .allSatisfy(
            message -> assertThat(message.getAttributesMap()).containsEntry("Kind", "test"));
  }

  private PubSubClientImpl createClient() {
    return new PubSubClientImpl(() -> mockPublisher, objectMapper);
  }

  private record TestPayload(String key) {}
}
