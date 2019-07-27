package io.smallrye.reactive.messaging.kinesis;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.processors.AsyncProcessor;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

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
        SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                .consumerARN(streamArn)
                .shardId(shardId)
                .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
                .build();

        AsyncProcessor<Message<?>> upstream = AsyncProcessor.create();
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
                .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
                .onEventStream(
                        p -> Flowable.fromPublisher(p)
                                .ofType(SubscribeToShardEvent.class)
                                .flatMapIterable(SubscribeToShardEvent::records)
                                .map(this::toMessage)
                                .subscribe(upstream::onNext))
                .build();
        kinesisClient.subscribeToShard(request, responseHandler);
        return ReactiveStreams.fromPublisher(upstream);
    }

    private Message<?> toMessage(Record record) {
        return Message.of(record.data().asByteArray());
    }
}
