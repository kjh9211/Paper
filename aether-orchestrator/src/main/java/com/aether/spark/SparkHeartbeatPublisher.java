package com.aether.spark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SparkHeartbeatPublisher {
    private static final Logger log = Logger.getLogger(SparkHeartbeatPublisher.class.getName());
    private static final int HEARTBEAT_INTERVAL_SEC = 5;

    private final String sparkId;
    private final JedisPool jedisPool;
    private final UserMigrationService migrationService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "spark-heartbeat"));

    public SparkHeartbeatPublisher(String sparkId, JedisPool jedisPool) {
        this.sparkId = sparkId;
        this.jedisPool = jedisPool;
        this.migrationService = new UserMigrationService(sparkId, jedisPool);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::publishHeartbeat, 0, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);
        migrationService.start();
    }

    public void stop() {
        scheduler.shutdownNow();
        migrationService.stop();
    }

    private void publishHeartbeat() {
        try (Jedis jedis = jedisPool.getResource()) {
            long now = Instant.now().getEpochSecond();
            jedis.hset("spark:registry:" + sparkId, Map.of(
                "heartbeat_ts", String.valueOf(now),
                "pid", String.valueOf(ProcessHandle.current().pid())
            ));
            jedis.publish("channel:heartbeat", sparkId + ":alive");
        } catch (Exception e) {
            log.warning("heartbeat publish failed: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String sparkId = System.getenv().getOrDefault("SPARK_ID", "spark-dev");
        int port = Integer.parseInt(System.getenv().getOrDefault("SPARK_PORT", "8101"));
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

        log.info("spark boot id=" + sparkId + " port=" + port + " redis=" + redisHost + ":" + redisPort);

        JedisPool pool = new JedisPool(redisHost, redisPort);
        SparkHeartbeatPublisher publisher = new SparkHeartbeatPublisher(sparkId, pool);
        publisher.start();
        publisher.migrationService.upsertLocalUser("sample-user", "{\"region\":\"spawn\",\"hp\":20}");

        try (Jedis jedis = pool.getResource()) {
            jedis.subscribe(new redis.clients.jedis.JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    if ("cmd:shutdown:all".equals(message) || ("cmd:shutdown:" + sparkId).equals(message)) {
                        publisher.stop();
                        pool.close();
                        unsubscribe();
                    }
                    if (message.startsWith("cmd:sync:")) {
                        log.info("sync requested key=" + message.substring("cmd:sync:".length()));
                    }
                    if (message.startsWith("cmd:move-user:")) {
                        String payload = message.substring("cmd:move-user:".length());
                        String[] parts = payload.split(":", 3);
                        if (parts.length == 3) {
                            String userId = parts[0];
                            String targetSparkId = parts[1];
                            String userJson = parts[2];
                            publisher.migrationService.publishMove(userId, targetSparkId, userJson);
                            log.info("user move signal published: userId=" + userId + ", target=" + targetSparkId);
                        }
                    }
                }
            }, "channel:orchestration");
        }

        log.info("spark shutdown complete id=" + sparkId);
    }
}
