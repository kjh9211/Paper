package com.aether.spark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public final class UserMigrationService {
    private static final Logger log = Logger.getLogger(UserMigrationService.class.getName());

    public static final String CH_USER_MOVE = "channel:user-move";
    public static final String KEY_USER_DATA = "nexus:user:";

    private final String sparkId;
    private final JedisPool jedisPool;
    private final Map<String, String> localUserStore = new ConcurrentHashMap<>();
    private final ExecutorService subscriberThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "user-move-subscriber"));

    private volatile boolean running;
    private JedisPubSub pubSub;

    public UserMigrationService(String sparkId, JedisPool jedisPool) {
        this.sparkId = sparkId;
        this.jedisPool = jedisPool;
    }

    public void start() {
        running = true;
        subscriberThread.submit(this::subscribeLoop);
    }

    public void stop() {
        running = false;
        if (pubSub != null && pubSub.isSubscribed()) {
            pubSub.unsubscribe();
        }
        subscriberThread.shutdownNow();
    }

    public void upsertLocalUser(String userId, String data) {
        localUserStore.put(userId, data);
    }

    public String getLocalUser(String userId) {
        return localUserStore.get(userId);
    }

    public void publishMove(String userId, String targetSparkId, String userData) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(KEY_USER_DATA + userId, userData);
            jedis.publish(CH_USER_MOVE, userId + ":" + targetSparkId);
        }
    }

    private void subscribeLoop() {
        pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                if (!CH_USER_MOVE.equals(channel)) {
                    return;
                }
                int sep = message.indexOf(':');
                if (sep < 1) {
                    return;
                }
                String userId = message.substring(0, sep);
                String targetSpark = message.substring(sep + 1);
                if (!sparkId.equals(targetSpark)) {
                    return;
                }
                overwriteFromSharedRedis(userId);
            }
        };

        while (running) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(pubSub, CH_USER_MOVE);
            } catch (Exception e) {
                if (running) {
                    log.warning("user move subscriber reconnecting: " + e.getMessage());
                    sleep(1500L);
                }
            }
        }
    }

    private void overwriteFromSharedRedis(String userId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = jedis.get(KEY_USER_DATA + userId);
            if (data == null) {
                return;
            }
            localUserStore.put(userId, data);
            log.info("user migrated and overwritten: userId=" + userId + ", target=" + sparkId);
        } catch (Exception e) {
            log.warning("failed to apply migrated user data: " + e.getMessage());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
