package io.smallrye.reactive.messaging.kinesis;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.kinesis.base.MapBasedConfig;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ProducingBean {

  @Outgoing("niki-test-out")
  public Publisher<Integer> source() {
    return Flowable.range(0, 50);
  }

  @Produces
  public Config myKinesisSinkConfig() {
    String prefix = "kinesis.messaging.incoming.";
    Map<String, Object> config = new HashMap<>();
    config.put(prefix + "connector", KinesisConnector.CONNECTOR_NAME);
    config.put(prefix + "stream", "niki-test");
    config.put("partitionKey", "app-1");
    return new MapBasedConfig(config);
  }

  @Produces
  @ConfigProperty(name = "aws.region")
  public String awsRegion() {
    return "ap-south-1";
  }
}
