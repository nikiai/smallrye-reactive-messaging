package io.smallrye.reactive.messaging.kinesis;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of config source selecting a set of property based on the {@code mp-config} system property.
 */
public class VarConfigSource implements ConfigSource {

    private Map<String, String> INCOMING_BEAN_CONFIG;
    private Map<String, String> OUTGOING_BEAN_CONFIG;

    private void init() {
        INCOMING_BEAN_CONFIG = new HashMap<>();
        OUTGOING_BEAN_CONFIG = new HashMap<>();

        String prefix = "kinesis.messaging.incoming.data.";
        INCOMING_BEAN_CONFIG.put(prefix + "streamName", "data");
        INCOMING_BEAN_CONFIG.put(prefix + "connector", KinesisConnector.CONNECTOR_NAME);
        INCOMING_BEAN_CONFIG.put("aws-region","ap-south-1");

        prefix = "kinesis.messaging.outgoing.sink.";
        OUTGOING_BEAN_CONFIG.put(prefix + "streamName", "sink");
        OUTGOING_BEAN_CONFIG.put(prefix + "connector", KinesisConnector.CONNECTOR_NAME);
        INCOMING_BEAN_CONFIG.put("aws-region","ap-south-1");
    }

    @Override
    public Map<String, String> getProperties() {
        init();
        String property = System.getProperty("mp-config");
        if ("incoming".equalsIgnoreCase(property)) {
            return INCOMING_BEAN_CONFIG;
        }
        if ("outgoing".equalsIgnoreCase(property)) {
            return OUTGOING_BEAN_CONFIG;
        }
        return Collections.emptyMap();
    }

    @Override
    public String getValue(String propertyName) {
        return getProperties().get(propertyName);
    }

    @Override
    public String getName() {
        return "var-config-source";
    }
}
