package io.smallrye.reactive.messaging.kinesis;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

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
  }
}
