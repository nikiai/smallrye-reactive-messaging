package io.smallrye.reactive.messaging.kinesis;

import io.vertx.reactivex.core.Vertx;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
@Connector(KinesisConnector.CONNECTOR_NAME)
public class KinesisConnector implements OutgoingConnectorFactory, IncomingConnectorFactory {

  static final String CONNECTOR_NAME = "smallrye.vertx.kinesis";
  private List<KinesisSource> sources = new CopyOnWriteArrayList<>();
  private List<KinesisSink> sinks = new CopyOnWriteArrayList<>();

  @Inject Instance<Vertx> instanceOfVertx;

  private boolean internalVertxInstance = false;
  private Vertx vertx;

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    sources.forEach(KinesisSource::closeQuietly);
    sinks.forEach(KinesisSink::closeQuietly);
    if (internalVertxInstance) {
      vertx.close();
    }
  }

  @PostConstruct
  void init() {
    if (instanceOfVertx.isUnsatisfied()) {
      internalVertxInstance = true;
      this.vertx = Vertx.vertx();
    } else {
      this.vertx = instanceOfVertx.get();
    }
  }

  @Override
  public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
    return new KinesisSource(vertx, config).source();
  }

  @Override
  public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
    return new KinesisSink(vertx, config).sink();
  }
}
