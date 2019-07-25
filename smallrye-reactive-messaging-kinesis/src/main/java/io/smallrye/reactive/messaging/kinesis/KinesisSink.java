package io.smallrye.reactive.messaging.kinesis;

import io.vertx.reactivex.core.Vertx;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class KinesisSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSink.class);

  private final Vertx vertx;
  private final KinesisAsyncClient kinesisClient;
  private final String stream;
  private final String partitionKey;
  private final SubscriberBuilder<? extends Message<?>, Void> subscriber;


  KinesisSink(Vertx vertx, Config config) {
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
    this.stream = config.getOptionalValue("stream", String.class).orElse(null);
    if (this.stream == null) {
      LOGGER.warn(
          "No default stream configured, only sending messages with an explicit stream set");
    }
    this.partitionKey = config.getOptionalValue("partitionKey", String.class).orElse(null);
    this.subscriber = ReactiveStreams.<Message<?>>builder()
      .flatMapCompletionStage(
        msg -> {
          CompletableFuture<Message> future = new CompletableFuture<>();
          sendMessage(msg, this.partitionKey);
          return future;
        })
      .ignore();
  }

  SubscriberBuilder<? extends Message<?>, Void> sink() {
    return subscriber;
  }

  private void sendMessage(Message<?> message, String partitionKey) throws RuntimeException {
    if (null == kinesisClient) {
      throw new RuntimeException("AmazonKinesisAsync is not initialized");
    }

    Consumer<PutRecordRequest.Builder> builderConsumer =
        recordBuilder -> {
          PutRecordRequest record;
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

            String actualTopicToBeUSed = this.stream;
            if (km.getStreamName() != null) {
              actualTopicToBeUSed = km.getStreamName();
            }

            if (km.getExplicitHashKey() != null) {
              recordBuilder.explicitHashKey(km.getExplicitHashKey());
            }

            if (km.getSequenceNumberForOrdering() != null) {
              recordBuilder.sequenceNumberForOrdering(km.getSequenceNumberForOrdering());
            }
            if (actualTopicToBeUSed == null) {
              LOGGER.error("Ignoring message - no stream set");
            } else {
              try {
                record =
                    recordBuilder
                        .data(SdkBytes.fromByteArray(serializeObject(km.getPayload())))
                        .build();
                LOGGER.info(
                    "Sending message {} to Kinesis stream '{}'", message, record.streamName());
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
              LOGGER.info(
                  "Sending message {} to Kinesis stream '{}'", message, record.streamName());
            } catch (IOException e) {
              LOGGER.error("Ignoring message - cannot serialize the object");
            }
          }
        };

    try {
      CompletableFuture<PutRecordResponse> future = kinesisClient.putRecord(builderConsumer);
      future.whenComplete(
          (result, e) ->
              vertx.runOnContext(
                  none -> {
                    if (e != null) {
                      LOGGER.error("Message delivery failed ...1");
                      e.printStackTrace();
                    } else {
                      String sequenceNumber = result.sequenceNumber();
                      LOGGER.debug("Message sequence number: " + sequenceNumber);
                    }
                  }));
    } catch (Exception exc) {
      LOGGER.error("Message delivery failed ...2");
      exc.printStackTrace();
    }
  }

  private static byte[] serializeObject(Object obj) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
    oos.writeObject(obj);
    oos.flush();
    byte[] bytes = bytesOut.toByteArray();
    bytesOut.close();
    oos.close();
    return bytes;
  }

  void closeQuietly() {
    if (kinesisClient != null) {
      kinesisClient.close();
    }
  }
}
