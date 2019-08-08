package io.smallrye.reactive.messaging.kinesis;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.kinesis.base.KinesisTestBase;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class KinesisSinkTest extends KinesisTestBase {

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testProducingMessagesSentToKinesis() throws InterruptedException, ExecutionException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingBean.class);
    container = weld.initialize();
    String myStreamName = "niki-test-out";

    final Config config = container.select(Config.class).get();
    final KinesisConnector connector =
        container.select(KinesisConnector.class, ConnectorLiteral.of("smallrye-kinesis")).get();
    final Subscriber subscriber = connector.getSubscriberBuilder(config).build();

    //noinspection unchecked
    final Flowable<? extends Message<?>> result = Flowable.range(0, 50).map(m -> KinesisMessage.of("app-1",m));
    result.subscribe(subscriber);
    Assert.assertFalse(result.isEmpty().blockingGet());

    final KinesisAsyncClient client = connector.getClient();
    DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder().streamName(myStreamName).build();
    final List<Shard> shards = client.describeStream(describeStreamRequest).get().streamDescription().shards();

    GetShardIteratorRequest getShardIteratorRequest =GetShardIteratorRequest.builder()
      .streamName(myStreamName)
      .shardIteratorType("TRIM_HORIZON")
      .shardId(shards.get(0).shardId()).build();

    String shardIterator;
    final GetShardIteratorResponse getShardIteratorResult = client.getShardIterator(getShardIteratorRequest).get();
    shardIterator = getShardIteratorResult.shardIterator();

    final GetRecordsRequest build = GetRecordsRequest.builder()
      .shardIterator(shardIterator).build();
    final List<Record> records = client.getRecords(build).get().records();
    assertThat(records.size()).isEqualTo(51);
  }

  @Test
  public void testABeanProducingMessagesSentToKinesis() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingBean.class);
    container = weld.initialize();
    TimeUnit.SECONDS.sleep(200);
  }

  @Test
  public void testABeanConsumingMessagesSentToKinesis() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ConsumptionBean.class);
    container = weld.initialize();
  }
}
