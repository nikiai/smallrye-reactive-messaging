package io.smallrye.reactive.messaging.kinesis;

import io.reactivex.Flowable;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ProducingBean {

    @Incoming("niki-test")
    @Outgoing("niki-test-out")
    public Message<Integer> process(Message<Integer> input) {
        return Message.of(input.getPayload() + 1, input::ack);
    }

    @Outgoing("niki-test")
    public Publisher<Integer> source() {
        return Flowable.range(0, 100);
    }

    @Produces
    public Config myKinesisSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("partitionKey","app-1");
        return new MapBasedConfig(config);
    }

}
