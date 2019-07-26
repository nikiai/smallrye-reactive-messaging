package io.smallrye.reactive.messaging.kinesis;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class KinesisSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);

  private final Vertx vertx;
  private final KinesisAsyncClient kinesisClient;
  private final String stream;
  private final String partitionKey;
//  private final PublisherBuilder<? extends Message<?>> source;

  KinesisSource(Vertx vertx, Config config) {
    this.vertx = Objects.requireNonNull(vertx, "Vert.x instance must not be `null`");
    ClientAsyncConfiguration clientConfiguration = ClientAsyncConfiguration.builder().build();
    AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
    Region region =
        Region.of(ObjectUtils.firstNonNull(config.getValue("region", String.class), "ap-south-1"));
    this.kinesisClient =
        KinesisAsyncClient.builder()
            .asyncConfiguration(clientConfiguration)
            .credentialsProvider(awsCredentialsProvider)
            .region(region)
            .build();
    this.stream = config.getOptionalValue("streamName", String.class).orElse(null);
    if (this.stream == null) {
      LOGGER.warn(
          "No default stream configured, only sending messages with an explicit stream set");
    }
    this.partitionKey = config.getOptionalValue("partitionKey", String.class).orElse(null);

    SubscribeToShardResponseHandler responseHandler =
      SubscribeToShardResponseHandler.builder()
        .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
        .onEventStream(
          p ->
            Flowable.fromPublisher(p)
              .ofType(SubscribeToShardEvent.class)
              .flatMapIterable(SubscribeToShardEvent::records)
              .limit(1000)
              .buffer(25)
              .subscribe(e -> System.out.println("Record batch = " + e)))
        .build();
    Consumer<SubscribeToShardRequest.Builder> builderConsumer = requestBuilder -> {
      requestBuilder.consumerARN("");
      requestBuilder.shardId("");
      requestBuilder.startingPosition(StartingPosition.builder()
        .sequenceNumber("")
        .timestamp(Instant.now())
        .type("")
        .build());
    };
    kinesisClient.subscribeToShard(builderConsumer, responseHandler);
  }

  PublisherBuilder<? extends Message<?>> source() {
    return null;
  }

  void closeQuietly() {
    if (kinesisClient != null) {
      kinesisClient.close();
    }
  }
}
