package io.smallrye.reactive.messaging.kinesis.base;

import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.extension.StreamProducer;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.kinesis.KinesisConnector;
import org.jboss.weld.environment.se.Weld;

public class KinesisTestBase {

    public static Weld baseWeld() {
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
