package com.aether.orchestrator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Redis Pub/Sub + 주기적 Key 폴링을 결합한 Heartbeat 모니터.
 *
 * ┌─────────────────────────────────────────────────────────────┐
 * │  Spark                        Redis (Nexus)                 │
 * │  ────                         ─────────────                 │
 * │  HSET spark:registry:{id}  ──▶  heartbeat, pid, port, ts   │
 * │  PUBLISH channel:heartbeat ──▶  "{id}:alive"               │
 * └─────────────────────────────────────────────────────────────┘
 *
 * Aether(HeartbeatMonitor)는:
 *   1) channel:heartbeat를 Subscribe해서 실시간 alive 신호 수신
 *   2) 별도 스케줄러로 spark:registry:{id} 키를 주기적으로 조회,
 *      마지막 ts가 DEAD_THRESHOLD 이상 오래됐으면 onDead 콜백 호출
 */
public class HeartbeatMonitor {

    private static final Logger log = Logger.getLogger(HeartbeatMonitor.class.getName());

    /* ─── Redis 채널/키 상수 ─── */
    public static final String CH_HEARTBEAT     = "channel:heartbeat";
    public static final String CH_ORCHESTRATION = "channel:orchestration";
    public static final String KEY_REGISTRY     = "spark:registry:";   // + sparkId

    /* ─── 타임아웃 설정 ─── */
    private static final long HEARTBEAT_INTERVAL_SEC = 5;    // Spark가 보내는 주기
    private static final long DEAD_THRESHOLD_SEC      = 15;  // 이 시간 무소식 → DEAD

    private final JedisPool          jedisPool;
    private final Map<String, Instant> lastSeenMap = new ConcurrentHashMap<>();

    // 콜백: sparkId 를 인자로 받음
    private final Consumer<String> onDead;      // Spark 사망 감지 시
    private final Consumer<String> onRecovered; // 재연결 감지 시

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "heartbeat-checker"));

    private final ExecutorService subscriberThread =
            Executors.newSingleThreadExecutor(
                r -> new Thread(r, "heartbeat-subscriber"));

    private volatile boolean running = false;
    private JedisPubSub pubSub;

    // ──────────────────────────────────────────────
    //  생성자
    // ──────────────────────────────────────────────
    public HeartbeatMonitor(JedisPool jedisPool,
                            Consumer<String> onDead,
                            Consumer<String> onRecovered) {
        this.jedisPool    = jedisPool;
        this.onDead       = onDead;
        this.onRecovered  = onRecovered;
    }

    // ──────────────────────────────────────────────
    //  시작
    // ──────────────────────────────────────────────
    public void start() {
        running = true;

        // 1) Pub/Sub 구독 (블로킹이므로 별도 스레드)
        subscriberThread.submit(this::runSubscriber);

        // 2) 주기적 Dead 체크 스케줄러
        scheduler.scheduleAtFixedRate(
            this::checkDeadSparks,
            HEARTBEAT_INTERVAL_SEC,
            HEARTBEAT_INTERVAL_SEC,
            TimeUnit.SECONDS
        );

        log.info("[HeartbeatMonitor] 시작됨. Dead 임계값=" + DEAD_THRESHOLD_SEC + "s");
    }

    // ──────────────────────────────────────────────
    //  Pub/Sub 구독 루프
    // ──────────────────────────────────────────────
    private void runSubscriber() {
        pubSub = new JedisPubSub() {

            @Override
            public void onMessage(String channel, String message) {
                // message 형식: "{sparkId}:alive"
                if (CH_HEARTBEAT.equals(channel) && message.endsWith(":alive")) {
                    String sparkId = message.replace(":alive", "");
                    boolean isNew = !lastSeenMap.containsKey(sparkId);
                    lastSeenMap.put(sparkId, Instant.now());

                    if (isNew) {
                        log.info("[HeartbeatMonitor] 신규 Spark 감지: " + sparkId);
                        onRecovered.accept(sparkId);
                    }
                    log.fine("[HeartbeatMonitor] Heartbeat 수신: " + sparkId);
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                log.info("[HeartbeatMonitor] 채널 구독 완료: " + channel);
            }
        };

        while (running) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(pubSub, CH_HEARTBEAT);   // 블로킹
            } catch (Exception e) {
                if (running) {
                    log.warning("[HeartbeatMonitor] Pub/Sub 연결 끊김. 3초 후 재연결...");
                    sleep(3_000);
                }
            }
        }
    }

    // ──────────────────────────────────────────────
    //  Dead 체크 (스케줄러 호출)
    // ──────────────────────────────────────────────
    private void checkDeadSparks() {
        Instant now = Instant.now();

        lastSeenMap.forEach((sparkId, lastSeen) -> {
            long silentSec = now.getEpochSecond() - lastSeen.getEpochSecond();

            if (silentSec >= DEAD_THRESHOLD_SEC) {
                log.warning(String.format(
                    "[HeartbeatMonitor] DEAD 감지! sparkId=%s | 마지막 수신=%ds 전",
                    sparkId, silentSec));

                // Redis Hash도 직접 조회해서 이중 확인
                if (isRegistryStale(sparkId)) {
                    lastSeenMap.remove(sparkId);  // 추가 알림 방지
                    onDead.accept(sparkId);
                }
            }
        });
    }

    /**
     * Redis의 spark:registry:{id}에서 heartbeat 타임스탬프를 직접 조회.
     * Pub/Sub 수신 실패(네트워크 이슈)와 실제 Spark 사망을 구별하는 이중 검증.
     */
    private boolean isRegistryStale(String sparkId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String tsStr = jedis.hget(KEY_REGISTRY + sparkId, "heartbeat_ts");
            if (tsStr == null) return true;

            long registryTs = Long.parseLong(tsStr);
            long nowEpoch   = Instant.now().getEpochSecond();
            return (nowEpoch - registryTs) >= DEAD_THRESHOLD_SEC;

        } catch (Exception e) {
            log.severe("[HeartbeatMonitor] Redis 조회 실패: " + e.getMessage());
            return false;  // 불확실한 상황에서는 재시작 보수적으로 판단
        }
    }

    // ──────────────────────────────────────────────
    //  Spark 등록 (Aether가 Spark 기동 후 호출)
    // ──────────────────────────────────────────────
    /**
     * 새로 기동된 Spark를 모니터링 대상으로 등록한다.
     * lastSeenMap에 추가해 두면 첫 Heartbeat 이전에 Dead로 판정되는 것을 방지한다.
     */
    public void register(String sparkId) {
        lastSeenMap.put(sparkId, Instant.now());
        log.info("[HeartbeatMonitor] 등록: " + sparkId);
    }

    public void unregister(String sparkId) {
        lastSeenMap.remove(sparkId);
        log.info("[HeartbeatMonitor] 등록 해제: " + sparkId);
    }

    // ──────────────────────────────────────────────
    //  종료
    // ──────────────────────────────────────────────
    public void stop() {
        running = false;
        if (pubSub != null && pubSub.isSubscribed()) {
            pubSub.unsubscribe();
        }
        scheduler.shutdownNow();
        subscriberThread.shutdownNow();
        log.info("[HeartbeatMonitor] 종료됨.");
    }

    // ──────────────────────────────────────────────
    //  유틸
    // ──────────────────────────────────────────────
    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<String, Instant> getLastSeenMap() {
        return Map.copyOf(lastSeenMap);
    }
}
