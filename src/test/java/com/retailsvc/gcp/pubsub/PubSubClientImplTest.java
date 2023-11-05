package com.retailsvc.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PubSubClientImplTest {

  @Mock ObjectMapper objectMapper;
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
        arguments(testPayload, Map.of("attribute-1", "value-1")));
  }

  @ParameterizedTest
  @MethodSource("publishInput")
  void testCanPublish(Object payload, Map<String, String> attributes) throws Exception {
    lenient().when(objectMapper.writeValueAsBytes(any())).thenReturn(new byte[0]);
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

  //
  //  private void withScopedValues(Runnable runnable) {
  //    ScopedValue.where(CorrelationId.SCOPED_CORRELATION, correlationId)
  //        .where(TenantId.SCOPED_TENANT, TEST_RUNNER)
  //        .run(runnable);
  //  }

  private PubSubClientImpl createClient() {
    return new PubSubClientImpl(() -> mockPublisher, objectMapper);
  }

  private record TestPayload(String key) {}
}
