package io.smallrye.reactive.messaging.kinesis;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.concurrent.CompletableFuture;

class KinesisSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);

  private final KinesisAsyncClient kinesisClient;
  private final String streamArn;
  private final String shardId;

  KinesisSource(KinesisAsyncClient kinesisClient,Config config) {
    this.kinesisClient = kinesisClient;
    this.streamArn = config.getOptionalValue("streamArn", String.class).orElse(null);
    if (this.streamArn == null) {
      LOGGER.warn(
          "No default stream configured, only sending messages with an explicit stream set");
    }
    this.shardId = config.getOptionalValue("shardId", String.class).orElse(null);
  }

  PublisherBuilder<? extends Message<?>> source() {
    SubscribeToShardRequest request = SubscribeToShardRequest.builder()
      .consumerARN(streamArn)
      .shardId(shardId)
      .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
      .build();
    return ReactiveStreams.fromCompletionStage(responseHandlerBuilder(kinesisClient,request));
  }

  private static CompletableFuture<Message<?>> responseHandlerBuilder(KinesisAsyncClient client, SubscribeToShardRequest request) {
    SubscribeToShardResponseHandler.Visitor visitor = SubscribeToShardResponseHandler.Visitor
      .builder()
      .onSubscribeToShardEvent(e -> System.out.println("Received subscribe to shard event " + e))
      .build();

    SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
      .builder()
      .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
      .subscriber(visitor)
      .build();
    // FIXME hack for returning a message to PublisherBuilder
    return client.subscribeToShard(request, responseHandler).thenApply(x -> KinesisMessage.of("test","test"));
  }

}
