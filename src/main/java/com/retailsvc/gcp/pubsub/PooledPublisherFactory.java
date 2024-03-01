package com.retailsvc.gcp.pubsub;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory to create a publisher backed by a pool of gRPC channels. A pooled publisher can handle
 * higher concurrent publish RPCs on the API. The created pool is dynamic and will scale in and out
 * depending on load.
 *
 * <p>The publisher is configured to use virtual threads to reduce memory footprint.
 *
 * @param initialChannelCount the initial number of gRPC channels in the pool
 * @param maxChannelCount the max number of gRPC channels in the pool
 * @param maxRpcsPerChannel the max number of concurrent RPCs allowed on one channel
 * @author sasjo
 */
public record PooledPublisherFactory(
    int initialChannelCount, int maxChannelCount, int maxRpcsPerChannel)
    implements PublisherFactory {

  /** Default initial pool size. */
  private static final int DEFAULT_POOL_INITIAL_SIZE = 2;

  /** Default max pool size. Sized to support 500 concurrent API calls. */
  private static final int DEFAULT_POOL_MAX_SIZE = 10;

  /** Default pool max RPCs per channel. */
  private static final int DEFAULT_POOL_MAX_RPC_PER_CHANNEL = 50;

  /**
   * Create a default pooled publisher that can support up to 500 concurrent requests.
   *
   * @return a publisher pool that can support up to 500 concurrent requests.
   */
  public static PooledPublisherFactory defaultPool() {
    return new PooledPublisherFactory(
        DEFAULT_POOL_INITIAL_SIZE, DEFAULT_POOL_MAX_SIZE, DEFAULT_POOL_MAX_RPC_PER_CHANNEL);
  }

  @Override
  public Publisher.Builder newBuilder(TopicName topic) {
    return Publisher.newBuilder(topic)
        .setChannelProvider(
            InstantiatingGrpcChannelProvider.newBuilder()
                .setChannelPoolSettings(
                    ChannelPoolSettings.builder()
                        .setMaxRpcsPerChannel(maxRpcsPerChannel)
                        .setInitialChannelCount(initialChannelCount)
                        .setMinRpcsPerChannel(1)
                        .setMaxChannelCount(maxChannelCount)
                        .setPreemptiveRefreshEnabled(true)
                        .build())
                .setExecutor(
                    Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("pubsub-channel-", 0).factory()))
                .build())
        .setExecutorProvider(new VirtualExecutorProvider());
  }

  private static class VirtualExecutorProvider implements ExecutorProvider {
    @Override
    public boolean shouldAutoClose() {
      return true;
    }

    @Override
    public ScheduledExecutorService getExecutor() {
      return Executors.newScheduledThreadPool(
          1, Thread.ofVirtual().name("pubsub-executor-", 0).factory());
    }
  }
}
