package io.smallrye.reactive.messaging.http;

import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

public class HttpConnectorConfig implements Config {
    private final Map<String, Object> map;
    private final String prefix;

    public HttpConnectorConfig(String name, String type, String url) {
        map = new HashMap<>();
        prefix = "mp.messaging." + type + "." + name + ".";
        map.put(prefix + "connector", HttpConnector.CONNECTOR_NAME);
        if (url != null) {
            map.put(prefix + "url", url);
        }
    }

    public HttpConnectorConfig(String name, Map<String, Object> conf) {
        prefix = "mp.messaging.outgoing." + name + ".";
        this.map = conf;
        map.put(prefix + "connector", HttpConnector.CONNECTOR_NAME);
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType)
                .orElseThrow(() -> new NoSuchElementException("Configuration not found"));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        T value = (T) map.get(propertyName);
        return Optional.ofNullable(value);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return map.keySet();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return Collections.emptyList();
    }

    public Config converter(String className) {
        map.put(prefix + "converter", className);
        return this;
    }
}
