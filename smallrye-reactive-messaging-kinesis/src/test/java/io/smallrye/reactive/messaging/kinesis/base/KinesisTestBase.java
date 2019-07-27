package io.smallrye.reactive.messaging.kinesis.base;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.smallrye.reactive.messaging.kinesis.ConsumptionBean;
import io.smallrye.reactive.messaging.kinesis.KinesisConnector;
import io.smallrye.reactive.messaging.kinesis.ProducingBean;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.weld.environment.se.Weld;

import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.extension.StreamProducer;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;

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
