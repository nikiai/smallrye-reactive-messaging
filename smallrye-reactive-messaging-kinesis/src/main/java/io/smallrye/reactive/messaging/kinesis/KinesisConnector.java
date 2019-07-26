package io.smallrye.reactive.messaging.kinesis;

import io.vertx.reactivex.core.Vertx;
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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Optional;

@ApplicationScoped
@Connector(KinesisConnector.CONNECTOR_NAME)
public class KinesisConnector implements OutgoingConnectorFactory, IncomingConnectorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConnector.class);
  static final String CONNECTOR_NAME = "smallrye-kinesis";

  private KinesisAsyncClient client;

  @Inject
  @ConfigProperty(name = "aws-profile")
  private Optional<String> awsProfile;

  @Inject
  @ConfigProperty(name = "aws-region", defaultValue = "ap-south-1")
  private String awsregion;

  @Inject private Instance<Vertx> instanceOfVertx;

  private boolean internalVertxInstance = false;
  private Vertx vertx;

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    if (internalVertxInstance) {
      vertx.close();
    }
  }

  @PostConstruct
  void init() {
    if (instanceOfVertx == null || instanceOfVertx.isUnsatisfied()) {
      internalVertxInstance = true;
      this.vertx = Vertx.vertx();
    } else {
      this.vertx = instanceOfVertx.get();
    }
  }

  KinesisConnector() {
    this.vertx = null;
  }

  private synchronized KinesisAsyncClient getClient(Config config) {
    if (client != null) {
      return client;
    }
    try {
      AwsCredentialsProvider awsCredentialsProvider =
          awsProfile.isPresent()
              ? ProfileCredentialsProvider.builder().profileName(awsProfile.get()).build()
              : DefaultCredentialsProvider.builder().build();
      Region region = Region.of(awsregion);
      this.client =
          KinesisAsyncClient.builder()
              .credentialsProvider(awsCredentialsProvider)
              .region(region)
              .build();
      return client;
    } catch (Exception e) {
      LOGGER.error("failed to connect to kinesis");
      throw new RuntimeException(e);
    }
  }

  @Override
  public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
    return new KinesisSource(client,config).source();
  }

  @Override
  public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
    return new KinesisSink(client,config).sink();
  }

  @PreDestroy
  public synchronized void close() {
    if (client != null) {
      client.close();
    }
  }
}
