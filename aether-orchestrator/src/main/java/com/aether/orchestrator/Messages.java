package com.aether.orchestrator;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class Messages {
    private final Map<String, String> values;

    private Messages(Map<String, String> values) {
        this.values = values;
    }

    public static Messages load(String langCode) {
        String file = "lang/" + langCode + ".yml";
        try (InputStream in = Messages.class.getClassLoader().getResourceAsStream(file)) {
            if (in == null) {
                throw new IllegalStateException(file + " not found");
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            @SuppressWarnings("unchecked")
            Map<String, String> map = new Yaml().load(text);
            return new Messages(map == null ? Map.of() : map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load " + file, e);
        }
    }

    public String get(String key) {
        return values.getOrDefault(key, key);
    }
}
