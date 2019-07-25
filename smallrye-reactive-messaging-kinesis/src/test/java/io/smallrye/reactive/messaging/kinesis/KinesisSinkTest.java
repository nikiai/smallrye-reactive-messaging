package io.smallrye.reactive.messaging.kinesis;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.entry;

public class KinesisSinkTest extends KinesisTestBase {

  private WeldContainer container;

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
  }

  @Test
  public void testABeanProducingMessagesSentToKafka() throws InterruptedException {
    Weld weld = baseWeld();
    weld.addBeanClass(ProducingBean.class);
    container = weld.initialize();

  }
}
