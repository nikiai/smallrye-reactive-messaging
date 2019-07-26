package io.smallrye.reactive.messaging.kinesis;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.smallrye.reactive.messaging.kinesis.KinesisUtils.serializeObject;

public class KinesisSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSink.class);

  private final KinesisAsyncClient kinesisClient;
  private final String stream;
  private final String partitionKey;
  private final int batchSize;
  private List<PutRecordsRequestEntry> messageBuffer;

  KinesisSink(KinesisAsyncClient kinesisClient, Config config) {
    this.kinesisClient = kinesisClient;
    this.stream = config.getOptionalValue("stream", String.class).orElse(null);
    if (this.stream == null) {
      LOGGER.warn(
          "No default stream configured, only sending messages with an explicit stream set");
    }
    this.partitionKey = config.getOptionalValue("partitionKey", String.class).orElse(null);
    this.batchSize = config.getOptionalValue("batchSize", Integer.class).orElse(1);
    this.messageBuffer = new ArrayList<>(batchSize);
  }

  SubscriberBuilder<? extends Message<?>, Void> sink() {
    return ReactiveStreams.<Message<?>>builder().flatMapCompletionStage(this::sendMessage).ignore();
  }

  private CompletionStage<PutRecordsResponse> sendMessage(Message<?> message)
      throws RuntimeException {
    if (null == kinesisClient) {
      throw new RuntimeException("AmazonKinesisAsync is not initialized");
    }
    PutRecordsRequestEntry.Builder recordBuilder = PutRecordsRequestEntry.builder();
    PutRecordsRequestEntry record;
    String actualTopicToBeUSed = this.stream;
    if (message instanceof KinesisMessage) {
      KinesisMessage km = ((KinesisMessage) message);
      if (this.stream == null && km.getStreamName() == null) {
        LOGGER.error("Ignoring message - no stream set");
      }

      if (km.getPartitionKey() != null) {
        recordBuilder.partitionKey(km.getPartitionKey());
      }

      if (km.getPartitionKey() == null && partitionKey != null) {
        recordBuilder.partitionKey(partitionKey);
      }

      if (km.getStreamName() != null) {
        actualTopicToBeUSed = km.getStreamName();
      }

      if (km.getExplicitHashKey() != null) {
        recordBuilder.explicitHashKey(km.getExplicitHashKey());
      }

      if (actualTopicToBeUSed == null) {
        LOGGER.error("Ignoring message - no stream set");
      } else {
        try {
          record =
              recordBuilder.data(SdkBytes.fromByteArray(serializeObject(km.getPayload()))).build();
          messageBuffer.add(record);
          LOGGER.info("Sending message {} to Kinesis stream '{}'", message, actualTopicToBeUSed);
        } catch (IOException e) {
          LOGGER.error("Ignoring message - cannot serialize the object");
        }
      }
    } else {
      try {
        record =
            recordBuilder
                .data(SdkBytes.fromByteArray(serializeObject(message.getPayload())))
                .build();
        messageBuffer.add(record);
        LOGGER.info("Sending message {} to Kinesis stream '{}'", message, actualTopicToBeUSed);
      } catch (IOException e) {
        LOGGER.error("Ignoring message - cannot serialize the object");
      }
    }

    //    if (messageBuffer.size() == batchSize) , how to process this in batch.
    return kinesisClient.putRecords(PutRecordsRequest.builder().records(messageBuffer).build());
    //    return new CompletableFuture<>();
  }
}
