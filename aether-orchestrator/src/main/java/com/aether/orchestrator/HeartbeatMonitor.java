package com.aether.orchestrator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class HeartbeatMonitor {
    private static final Logger log = Logger.getLogger(HeartbeatMonitor.class.getName());

    public static final String CH_HEARTBEAT = "channel:heartbeat";
    public static final String CH_ORCHESTRATION = "channel:orchestration";
    public static final String KEY_REGISTRY = "spark:registry:";

    private static final long HEARTBEAT_INTERVAL_SEC = 5;
    private static final long DEAD_THRESHOLD_SEC = 15;

    private final JedisPool jedisPool;
    private final Map<String, Instant> lastSeenMap = new ConcurrentHashMap<>();
    private final Consumer<String> onDead;
    private final Consumer<String> onRecovered;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "heartbeat-checker"));
    private final ExecutorService subscriberThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "heartbeat-subscriber"));

    private volatile boolean running;
    private JedisPubSub pubSub;

    public HeartbeatMonitor(JedisPool jedisPool, Consumer<String> onDead, Consumer<String> onRecovered) {
        this.jedisPool = jedisPool;
        this.onDead = onDead;
        this.onRecovered = onRecovered;
    }

    public void start() {
        this.running = true;
        this.subscriberThread.submit(this::runSubscriber);
        this.scheduler.scheduleAtFixedRate(this::checkDeadSparks, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    private void runSubscriber() {
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                if (CH_HEARTBEAT.equals(channel) && message.endsWith(":alive")) {
                    String sparkId = message.substring(0, message.length() - 6);
                    boolean isNew = !lastSeenMap.containsKey(sparkId);
                    lastSeenMap.put(sparkId, Instant.now());
                    if (isNew) {
                        onRecovered.accept(sparkId);
                    }
                }
            }
        };

        while (running) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(pubSub, CH_HEARTBEAT);
            } catch (Exception e) {
                if (running) {
                    log.warning("heartbeat subscriber reconnecting: " + e.getMessage());
                    sleep(3_000);
                }
            }
        }
    }

    private void checkDeadSparks() {
        Instant now = Instant.now();
        lastSeenMap.forEach((sparkId, lastSeen) -> {
            long silentSec = now.getEpochSecond() - lastSeen.getEpochSecond();
            if (silentSec >= DEAD_THRESHOLD_SEC && isRegistryStale(sparkId)) {
                lastSeenMap.remove(sparkId);
                onDead.accept(sparkId);
            }
        });
    }

    private boolean isRegistryStale(String sparkId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String ts = jedis.hget(KEY_REGISTRY + sparkId, "heartbeat_ts");
            if (ts == null) {
                return true;
            }
            long now = Instant.now().getEpochSecond();
            return now - Long.parseLong(ts) >= DEAD_THRESHOLD_SEC;
        } catch (Exception e) {
            log.warning("heartbeat registry check failed: " + e.getMessage());
            return false;
        }
    }

    public void register(String sparkId) {
        this.lastSeenMap.put(sparkId, Instant.now());
    }

    public void unregister(String sparkId) {
        this.lastSeenMap.remove(sparkId);
    }

    public void stop() {
        this.running = false;
        if (this.pubSub != null && this.pubSub.isSubscribed()) {
            this.pubSub.unsubscribe();
        }
        this.scheduler.shutdownNow();
        this.subscriberThread.shutdownNow();
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
