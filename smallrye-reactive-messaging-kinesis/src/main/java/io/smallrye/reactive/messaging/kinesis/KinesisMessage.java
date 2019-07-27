package io.smallrye.reactive.messaging.kinesis;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

public interface KinesisMessage<T> extends Message<T> {

    static <T> KinesisMessage<T> of(String key, T value) {
        return new SendingKinesisMessage<>(null, key, value, null, null);
    }

    static <T> KinesisMessage<T> withKeyAndValue(String key, T value) {
        return new SendingKinesisMessage<>(null, key, value, null, null);
    }

    static <T> KinesisMessage<T> of(String stream, String key, T value) {
        return new SendingKinesisMessage<>(stream, key, value, null, null);
    }

    static <T> KinesisMessage<T> of(String stream, String key, T value, String explicitHashKey) {
        return new SendingKinesisMessage<>(stream, key, value, explicitHashKey, null);
    }

    default KinesisMessage<T> withAck(Supplier<CompletionStage<Void>> ack) {
        throw new UnsupportedOperationException("Acknowledgment not yet supported");
    }

    T getPayload();

    String getPartitionKey();

    String getStreamName();

    String getExplicitHashKey();

}
