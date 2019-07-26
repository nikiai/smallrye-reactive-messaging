package io.smallrye.reactive.messaging.kinesis;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

class KinesisSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);

  private final KinesisAsyncClient kinesisClient;
  private final String streamArn;
  private final String shardId;

  KinesisSource(KinesisAsyncClient kinesisClient, Config config) {
    this.kinesisClient = kinesisClient;
    this.streamArn = config.getOptionalValue("streamArn", String.class).orElse(null);
    if (this.streamArn == null) {
      LOGGER.warn(
          "No default stream configured, only sending messages with an explicit stream set");
    }
    this.shardId = config.getOptionalValue("shardId", String.class).orElse(null);
  }

  PublisherBuilder<? extends Message<?>> source() {
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(streamArn)
            .shardId(shardId)
            .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
            .build();
    return responseHandlerBuilder(kinesisClient, request);
  }

  private static PublisherBuilder<Message<?>> responseHandlerBuilder(
      KinesisAsyncClient client, SubscribeToShardRequest request) {

    Flowable<Message<?>> upstream = Flowable.empty();
    io.reactivex.functions.Consumer<List<Record>> copier = e -> System.out.println(e.size());

    SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
      .builder()
      .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
      .onEventStream(p -> Flowable.fromPublisher(p)
        .ofType(SubscribeToShardEvent.class)
        .flatMapIterable(SubscribeToShardEvent::records)
        .limit(1000)
        .buffer(25)
        .subscribe(copier))
      .build();
    client.subscribeToShard(request, responseHandler);
    return ReactiveStreams.fromPublisher(upstream);
  }
}
