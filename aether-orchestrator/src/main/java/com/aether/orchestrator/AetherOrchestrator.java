package com.aether.orchestrator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class AetherOrchestrator {
    private static final Logger log = Logger.getLogger(AetherOrchestrator.class.getName());

    private final int sparkCount;
    private final int basePort;
    private final Path jarPath;
    private final String redisHost;
    private final int redisPort;
    private final long restartCooldownMs;
    private final Path instancesRootDir;
    private final Path sharedPluginsDir;
    private final SparkProcess.ConnectionMode connectionMode;
    private final String velocitySecret;

    private final Map<String, SparkProcess> sparkMap = new ConcurrentHashMap<>();
    private final Map<String, Long> restartCooldown = new ConcurrentHashMap<>();

    private JedisPool jedisPool;
    private HeartbeatMonitor heartbeatMonitor;
    private volatile boolean running;

    private AetherOrchestrator(Builder b) {
        this.sparkCount = b.sparkCount;
        this.basePort = b.basePort;
        this.jarPath = b.jarPath;
        this.redisHost = b.redisHost;
        this.redisPort = b.redisPort;
        this.restartCooldownMs = b.restartCooldownMs;
        this.instancesRootDir = b.instancesRootDir;
        this.sharedPluginsDir = b.sharedPluginsDir;
        this.connectionMode = b.connectionMode;
        this.velocitySecret = b.velocitySecret;
    }

    public void start() {
        this.running = true;
        initRedisPool();

        this.heartbeatMonitor = new HeartbeatMonitor(jedisPool, this::handleDeadSpark, this::handleRecoveredSpark);
        this.heartbeatMonitor.start();

        for (int i = 1; i <= sparkCount; i++) {
            String sparkId = "spark-" + i;
            int port = basePort + i;
            spawnSpark(sparkId, port);
        }

        startConflictSubscriber();
    }

    public void stop() {
        this.running = false;

        if (heartbeatMonitor != null) {
            heartbeatMonitor.stop();
        }

        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(HeartbeatMonitor.CH_ORCHESTRATION, "cmd:shutdown:all");
            } catch (Exception e) {
                log.warning("shutdown publish failed: " + e.getMessage());
            }
        }

        sparkMap.forEach((id, spark) -> spark.stopGracefully(15));
        sparkMap.clear();

        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    private void spawnSpark(String sparkId, int port) {
        SparkProcess spark = new SparkProcess(
            sparkId, port, jarPath, redisHost, redisPort,
            instancesRootDir, sharedPluginsDir, connectionMode, velocitySecret
        );
        try {
            spark.start();
            sparkMap.put(sparkId, spark);
            heartbeatMonitor.register(sparkId);

            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(HeartbeatMonitor.KEY_REGISTRY + sparkId, Map.of(
                    "pid", String.valueOf(spark.getPid()),
                    "port", String.valueOf(port),
                    "started_at", spark.getStartedAt().toString(),
                    "heartbeat_ts", String.valueOf(spark.getStartedAt().getEpochSecond())
                ));
            }
        } catch (IOException e) {
            log.warning("spark spawn failed: " + sparkId + " " + e.getMessage());
        }
    }

    private void handleDeadSpark(String sparkId) {
        long now = System.currentTimeMillis();
        long last = restartCooldown.getOrDefault(sparkId, 0L);
        if (now - last < restartCooldownMs) {
            return;
        }

        SparkProcess dead = sparkMap.get(sparkId);
        if (dead == null) {
            return;
        }

        restartCooldown.put(sparkId, now);
        dead.stopGracefully(5);
        heartbeatMonitor.unregister(sparkId);
        sparkMap.remove(sparkId);
        spawnSpark(sparkId, dead.getPort());
    }

    private void handleRecoveredSpark(String sparkId) {
        SparkProcess spark = sparkMap.get(sparkId);
        if (spark != null) {
            spark.setStatus(SparkProcess.SparkStatus.RUNNING);
        }
    }

    private void startConflictSubscriber() {
        Thread thread = new Thread(() -> {
            while (running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.subscribe(new redis.clients.jedis.JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            if (message.startsWith("event:conflict:")) {
                                handleConflict(message);
                            }
                        }
                    }, HeartbeatMonitor.CH_ORCHESTRATION);
                } catch (Exception e) {
                    if (running) {
                        sleep(2_000);
                    }
                }
            }
        }, "conflict-subscriber");
        thread.setDaemon(true);
        thread.start();
    }

    private void handleConflict(String message) {
        int thirdColon = message.indexOf(':', "event:conflict:".length());
        String sparkId = thirdColon > 0 ? message.substring("event:conflict:".length(), thirdColon) : "unknown";
        String dataKey = thirdColon > 0 ? message.substring(thirdColon + 1) : "nexus:data:state";

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.watch(dataKey, "nexus:data:version");
            String raw = jedis.get(dataKey);
            String version = jedis.get("nexus:data:version");

            String resolved = resolveConflict(raw, version, sparkId);
            var tx = jedis.multi();
            tx.set(dataKey, resolved == null ? "" : resolved);
            tx.incr("nexus:data:version");
            var result = tx.exec();

            if (result != null) {
                jedis.publish(HeartbeatMonitor.CH_ORCHESTRATION, "cmd:sync:" + dataKey);
            }
        }
    }

    protected String resolveConflict(String currentData, String version, String reporterId) {
        return currentData;
    }

    private void initRedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setTestOnBorrow(true);
        this.jedisPool = new JedisPool(config, redisHost, redisPort, 3000);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class Builder {
        private int sparkCount = 3;
        private int basePort = 8100;
        private Path jarPath;
        private String redisHost = "localhost";
        private int redisPort = 6379;
        private long restartCooldownMs = 10_000;
        private Path instancesRootDir = Path.of("instances");
        private Path sharedPluginsDir = Path.of("plugins");
        private SparkProcess.ConnectionMode connectionMode = SparkProcess.ConnectionMode.DIRECT;
        private String velocitySecret = "change-me";

        public Builder sparkCount(int sparkCount) {
            this.sparkCount = sparkCount;
            return this;
        }

        public Builder basePort(int basePort) {
            this.basePort = basePort;
            return this;
        }

        public Builder jarPath(Path jarPath) {
            this.jarPath = jarPath;
            return this;
        }

        public Builder redisHost(String redisHost) {
            this.redisHost = redisHost;
            return this;
        }

        public Builder redisPort(int redisPort) {
            this.redisPort = redisPort;
            return this;
        }

        public Builder restartCooldownSeconds(int seconds) {
            this.restartCooldownMs = Math.max(1, seconds) * 1_000L;
            return this;
        }

        public Builder instancesRootDir(Path instancesRootDir) {
            this.instancesRootDir = instancesRootDir;
            return this;
        }

        public Builder sharedPluginsDir(Path sharedPluginsDir) {
            this.sharedPluginsDir = sharedPluginsDir;
            return this;
        }

        public Builder connectionMode(String mode) {
            this.connectionMode = SparkProcess.ConnectionMode.from(mode);
            return this;
        }

        public Builder velocitySecret(String velocitySecret) {
            this.velocitySecret = velocitySecret;
            return this;
        }

        public AetherOrchestrator build() {
            if (jarPath == null) {
                throw new IllegalStateException("jarPath is required");
            }
            return new AetherOrchestrator(this);
        }
    }
}
