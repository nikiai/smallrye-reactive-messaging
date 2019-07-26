package io.smallrye.reactive.messaging.kinesis;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class SendingKinesisMessage<T> implements KinesisMessage<T> {

  private final String streamName;
  private final String partitionKey;
  private final T value;
  private final String explicitHashKey;
  private final Supplier<CompletionStage<Void>> ack;

  SendingKinesisMessage(
    String streamName,
    String partitionKey,
    T value,
    String explicitHashKey,
    Supplier<CompletionStage<Void>> ack) {
    this.streamName = streamName;
    this.partitionKey = partitionKey;
    this.value = value;
    this.explicitHashKey = explicitHashKey;
    this.ack = ack;
  }

  @Override
  public CompletionStage<Void> ack() {
    if (ack == null) {
      return CompletableFuture.completedFuture(null);
    } else {
      return ack.get();
    }
  }

  @Override
  public T getPayload() {
    return this.value;
  }

  @Override
  public String getPartitionKey() {
    return this.partitionKey;
  }

  @Override
  public String getStreamName() {
    return this.streamName;
  }

  @Override
  public String getExplicitHashKey() {
    return this.explicitHashKey;
  }

}
