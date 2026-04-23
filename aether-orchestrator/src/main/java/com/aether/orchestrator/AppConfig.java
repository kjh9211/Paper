package com.aether.orchestrator;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class AppConfig {
    public record Redis(String host, int port) {}
    public record Orchestrator(
        int sparkCount,
        int basePort,
        String sparkJarPath,
        int restartCooldownSeconds,
        String instancesRootDir,
        String sharedPluginsDir
    ) {}
    public record Network(String mode, String velocitySecret) {}
    public record Database(String type, H2 h2, Mysql mysql) {}
    public record H2(String url, String username, String password) {}
    public record Mysql(String host, int port, String database, String username, String password, String jdbcParams) {}

    private final Redis redis;
    private final Orchestrator orchestrator;
    private final Network network;
    private final Database database;
    private final String lang;

    private AppConfig(Redis redis, Orchestrator orchestrator, Network network, Database database, String lang) {
        this.redis = redis;
        this.orchestrator = orchestrator;
        this.network = network;
        this.database = database;
        this.lang = lang;
    }

    public static AppConfig load() {
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("config.yml")) {
            if (in == null) {
                throw new IllegalStateException("config.yml not found");
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, Object> root = new Yaml().load(text);

            Map<String, Object> redisMap = map(root, "redis");
            Redis redis = new Redis(
                str(redisMap, "host", "localhost"),
                integer(redisMap, "port", 6379)
            );

            Map<String, Object> orchMap = map(root, "orchestrator");
            Orchestrator orchestrator = new Orchestrator(
                integer(orchMap, "sparkCount", 3),
                integer(orchMap, "basePort", 8100),
                str(orchMap, "sparkJarPath", "spark-server.jar"),
                integer(orchMap, "restartCooldownSeconds", 10),
                str(orchMap, "instancesRootDir", "instances"),
                str(orchMap, "sharedPluginsDir", "plugins")
            );

            Map<String, Object> networkMap = map(root, "network");
            Network network = new Network(
                str(networkMap, "mode", "DIRECT"),
                str(networkMap, "velocitySecret", "change-me")
            );

            Map<String, Object> dbMap = map(root, "database");
            Map<String, Object> h2Map = map(dbMap, "h2");
            Map<String, Object> mysqlMap = map(dbMap, "mysql");
            Database database = new Database(
                str(dbMap, "type", "h2"),
                new H2(
                    str(h2Map, "url", "jdbc:h2:./aether;MODE=MySQL;DB_CLOSE_DELAY=-1"),
                    str(h2Map, "username", "sa"),
                    str(h2Map, "password", "")
                ),
                new Mysql(
                    str(mysqlMap, "host", "localhost"),
                    integer(mysqlMap, "port", 3306),
                    str(mysqlMap, "database", "aether"),
                    str(mysqlMap, "username", "root"),
                    str(mysqlMap, "password", ""),
                    str(mysqlMap, "jdbcParams", "useSSL=false&serverTimezone=Asia/Seoul&characterEncoding=UTF-8")
                )
            );

            return new AppConfig(redis, orchestrator, network, database, str(root, "lang", "ko"));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load config.yml", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> root, String key) {
        Object value = root.get(key);
        if (value instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        return Map.of();
    }

    private static String str(Map<String, Object> root, String key, String def) {
        Object value = root.get(key);
        return value == null ? def : value.toString();
    }

    private static int integer(Map<String, Object> root, String key, int def) {
        Object value = root.get(key);
        if (value == null) {
            return def;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    public Redis redis() {
        return redis;
    }

    public Orchestrator orchestrator() {
        return orchestrator;
    }

    public Database database() {
        return database;
    }

    public Network network() {
        return network;
    }

    public String lang() {
        return lang;
    }
}
