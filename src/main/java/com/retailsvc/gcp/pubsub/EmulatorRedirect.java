package com.retailsvc.gcp.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher.Builder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Based on <a
 * href="https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java">https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java</a>
 *
 * <p>Supports running the library with an emulated pubsub service
 *
 * @author thced
 */
class EmulatorRedirect {

  static final String PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST";

  private EmulatorRedirect() {}

  private static final Map<String, TransportChannelProvider> channelProviders =
      new ConcurrentHashMap<>();

  public static void redirect(Builder builder) {
    String hostPort = System.getProperty(PUBSUB_EMULATOR_HOST, System.getenv(PUBSUB_EMULATOR_HOST));

    TransportChannelProvider channelProvider =
        channelProviders.computeIfAbsent(hostPort, EmulatorRedirect::createChannelProvider);

    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    // Set the channel and credentials provider when creating a `Publisher`.
    builder.setChannelProvider(channelProvider);
    builder.setCredentialsProvider(credentialsProvider);
  }

  private static TransportChannelProvider createChannelProvider(String hostPort) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
  }
}
