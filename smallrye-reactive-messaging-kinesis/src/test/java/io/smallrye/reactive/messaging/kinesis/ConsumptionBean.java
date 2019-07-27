package io.smallrye.reactive.messaging.kinesis;

import io.smallrye.reactive.messaging.kinesis.base.MapBasedConfig;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ConsumptionBean {

  private List<Integer> list = new ArrayList<>();

  @Incoming("data")
  @Outgoing("sink")
  @Acknowledgment(Acknowledgment.Strategy.MANUAL)
  public Message<Integer> process(KinesisMessage<Integer> input) {
    return Message.of(input.getPayload() + 1, input::ack);
  }

  @Incoming("sink")
  public void sink(int val) {
    list.add(val);
  }

  @Produces
  public Config myKinesisSourceConfig() {
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
    return "localhost:9092";
  }

  public List<Integer> getResults() {
    return list;
  }
}
