package io.smallrye.reactive.messaging.kinesis;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Connector(KinesisConnector.CONNECTOR_NAME)
public class KinesisConnector implements OutgoingConnectorFactory, IncomingConnectorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConnector.class);
  static final String CONNECTOR_NAME = "smallrye-kinesis";

  private KinesisAsyncClient client;

  @Inject
  @ConfigProperty(name = "aws-region", defaultValue = "ap-south-1")
  private String awsregion;

  @PostConstruct
  void init() {
    this.client =
        KinesisAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .region(Region.of(awsregion))
            .build();
  }

  @Override
  public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
    return new KinesisSource(client, config).source();
  }

  @Override
  public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
    return new KinesisSink(client, config).sink();
  }

  @PreDestroy
  public synchronized void close() {
    if (client != null) {
      client.close();
    }
  }
}
