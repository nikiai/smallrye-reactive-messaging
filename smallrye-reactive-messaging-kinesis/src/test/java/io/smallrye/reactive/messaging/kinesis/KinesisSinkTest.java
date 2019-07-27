package io.smallrye.reactive.messaging.kinesis;

import io.smallrye.reactive.messaging.kinesis.base.KinesisTestBase;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.concurrent.TimeUnit;
public class KinesisSinkTest extends KinesisTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    @Test
    public void testABeanProducingMessagesSentToKinesis() throws InterruptedException {
        Weld weld = baseWeld();
        weld.addBeanClass(ProducingBean.class);
        container = weld.initialize();
        TimeUnit.SECONDS.sleep(20);
    }

  @Test
  public void testABeanConsumingMessagesSentToKinesis() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ConsumptionBean.class);
    container = weld.initialize();
  }

  private KinesisAsyncClient client(){
    return KinesisAsyncClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.builder().build())
      .region(Region.of("ap-south-1"))
      .build();
  }
}
