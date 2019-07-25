package io.smallrye.reactive.messaging.kinesis;

import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.extension.StreamProducer;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.kinesis.KinesisConnector;
import io.vertx.reactivex.core.Vertx;
import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;

public class KinesisTestBase {

    Vertx vertx;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown() {
        vertx.close();
    }

    static Weld baseWeld() {
        Weld weld = new Weld();
        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(StreamProducer.class);
        weld.addExtension(new ReactiveMessagingExtension());
        weld.addBeanClass(KinesisConnector.class);
        weld.disableDiscovery();
        return weld;
    }

}
