package com.aether.orchestrator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * ┌──────────────────────────────────────────────────────────────────┐
 * │                    Aether Orchestrator                           │
 * │                                                                  │
 * │  역할:                                                            │
 * │  - SparkProcess 인스턴스 풀 관리 (기동, 감시, 재시작, 종료)            │
 * │  - HeartbeatMonitor로부터 Dead/Recovered 콜백 수신                 │
 * │  - Redis를 통한 Conflict 중재 (channel:orchestration 구독)          │
 * │  - Graceful Shutdown 보장                                         │
 * └──────────────────────────────────────────────────────────────────┘
 *
 * 사용 예시:
 * <pre>
 *   AetherOrchestrator aether = new AetherOrchestrator.Builder()
 *       .sparkCount(3)
 *       .basePort(8100)
 *       .jarPath(Path.of("spark-server.jar"))
 *       .redisHost("localhost")
 *       .build();
 *
 *   aether.start();
 *   Runtime.getRuntime().addShutdownHook(new Thread(aether::stop));
 * </pre>
 */
public class AetherOrchestrator {

    private static final Logger log = Logger.getLogger(AetherOrchestrator.class.getName());

    /* ─── 설정 ─── */
    private final int    sparkCount;
    private final int    basePort;
    private final Path   jarPath;
    private final String redisHost;
    private final int    redisPort;

    /* ─── 런타임 상태 ─── */
    private final Map<String, SparkProcess> sparkMap = new ConcurrentHashMap<>();
    private JedisPool        jedisPool;
    private HeartbeatMonitor heartbeatMonitor;

    // Spark 재시작 시 연달아 무한 루프 방지용 쿨다운 추적
    private final Map<String, Long> restartCooldown = new ConcurrentHashMap<>();
    private static final long RESTART_COOLDOWN_MS = 10_000; // 10초

    private volatile boolean running = false;

    // ──────────────────────────────────────────────
    //  생성자 (Builder 패턴)
    // ──────────────────────────────────────────────
    private AetherOrchestrator(Builder b) {
        this.sparkCount = b.sparkCount;
        this.basePort   = b.basePort;
        this.jarPath    = b.jarPath;
        this.redisHost  = b.redisHost;
        this.redisPort  = b.redisPort;
    }

    // ──────────────────────────────────────────────
    //  시작
    // ──────────────────────────────────────────────
    public void start() {
        running = true;
        log.info("===== Aether 오케스트레이터 기동 시작 =====");

        // 1) Redis 연결 풀 초기화
        initRedisPool();

        // 2) HeartbeatMonitor 초기화 및 시작
        heartbeatMonitor = new HeartbeatMonitor(
            jedisPool,
            this::handleDeadSpark,       // Spark 사망 콜백
            this::handleRecoveredSpark   // Spark 재연결 콜백
        );
        heartbeatMonitor.start();

        // 3) Spark 인스턴스 일괄 기동
        for (int i = 1; i <= sparkCount; i++) {
            String sparkId = "spark-" + i;
            int    port    = basePort + i;
            spawnSpark(sparkId, port);
        }

        // 4) Redis Conflict 채널 구독
        startConflictSubscriber();

        log.info("===== Aether 기동 완료. Spark " + sparkCount + "개 실행 중 =====");
    }

    // ──────────────────────────────────────────────
    //  Spark 기동 (내부용)
    // ──────────────────────────────────────────────
    private void spawnSpark(String sparkId, int port) {
        SparkProcess spark = new SparkProcess(sparkId, port, jarPath, redisHost, redisPort);
        try {
            spark.start();
            sparkMap.put(sparkId, spark);
            heartbeatMonitor.register(sparkId);

            // Redis에 Spark 메타 정보 등록
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(HeartbeatMonitor.KEY_REGISTRY + sparkId, Map.of(
                    "pid",          String.valueOf(spark.getPid()),
                    "port",         String.valueOf(port),
                    "started_at",   spark.getStartedAt().toString(),
                    "heartbeat_ts", String.valueOf(spark.getStartedAt().getEpochSecond())
                ));
            }
        } catch (IOException e) {
            log.severe("[Aether] " + sparkId + " 기동 실패: " + e.getMessage());
        }
    }

    // ──────────────────────────────────────────────
    //  Heartbeat 콜백 처리
    // ──────────────────────────────────────────────

    /**
     * HeartbeatMonitor가 특정 Spark의 사망을 감지했을 때 호출.
     * 쿨다운 체크 후 재시작 수행.
     */
    private void handleDeadSpark(String sparkId) {
        log.warning("[Aether] DEAD 신호 수신: " + sparkId);

        // 쿨다운 확인 (연속 재시작 방지)
        long now      = System.currentTimeMillis();
        long lastTime = restartCooldown.getOrDefault(sparkId, 0L);
        if (now - lastTime < RESTART_COOLDOWN_MS) {
            log.warning("[Aether] " + sparkId + " 쿨다운 중. 재시작 보류.");
            return;
        }

        SparkProcess dead = sparkMap.get(sparkId);
        if (dead == null) return;

        log.info("[Aether] " + sparkId + " 재시작 시도...");
        restartCooldown.put(sparkId, now);

        dead.stopGracefully(5); // 혹시 좀비 프로세스라면 정리
        heartbeatMonitor.unregister(sparkId);
        sparkMap.remove(sparkId);

        // 같은 ID/포트로 재기동
        spawnSpark(sparkId, dead.getPort());
        log.info("[Aether] " + sparkId + " 재시작 완료.");
    }

    /**
     * 죽었던 Spark가 Heartbeat를 재전송할 때 (예: 수동 재기동) 호출.
     */
    private void handleRecoveredSpark(String sparkId) {
        SparkProcess spark = sparkMap.get(sparkId);
        if (spark != null) {
            spark.setStatus(SparkProcess.SparkStatus.RUNNING);
            log.info("[Aether] " + sparkId + " 복구 확인.");
        }
    }

    // ──────────────────────────────────────────────
    //  Conflict 중재 구독 (channel:orchestration)
    // ──────────────────────────────────────────────
    private void startConflictSubscriber() {
        Thread t = new Thread(() -> {
            while (running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.subscribe(new redis.clients.jedis.JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            // 메시지 형식: "event:conflict:{sparkId}:{dataKey}"
                            if (message.startsWith("event:conflict:")) {
                                handleConflict(message);
                            }
                        }
                    }, HeartbeatMonitor.CH_ORCHESTRATION);
                } catch (Exception e) {
                    if (running) {
                        log.warning("[Aether] Conflict 채널 재연결 중...");
                        sleep(2_000);
                    }
                }
            }
        }, "conflict-subscriber");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Conflict 중재 로직.
     * 실제 게임 로직에 맞게 customizeResolution()을 오버라이드하거나 교체할 것.
     *
     * message 예시: "event:conflict:spark-2:nexus:data:state"
     */
    private void handleConflict(String message) {
        String[] parts   = message.split(":", 4);
        String   sparkId = parts.length > 2 ? parts[2] : "unknown";
        String   dataKey = parts.length > 3 ? parts[3] : "nexus:data:state";

        log.info(String.format("[Aether] Conflict 중재 시작 | reporter=%s | key=%s",
            sparkId, dataKey));

        try (Jedis jedis = jedisPool.getResource()) {
            // WATCH → 읽기 → 중재 로직 → MULTI/EXEC (낙관적 락)
            jedis.watch(dataKey, "nexus:data:version");
            String  raw     = jedis.get(dataKey);
            String  version = jedis.get("nexus:data:version");

            String resolved = resolveConflict(raw, version, sparkId);

            var tx = jedis.multi();
            tx.set(dataKey, resolved);
            tx.incr("nexus:data:version");
            var result = tx.exec();

            if (result == null) {
                log.warning("[Aether] Conflict 중재 중 또 다른 충돌 발생. 재시도 필요.");
            } else {
                log.info("[Aether] Conflict 해결 완료. 동기화 신호 전파.");
                jedis.publish(HeartbeatMonitor.CH_ORCHESTRATION,
                              "cmd:sync:" + dataKey);
            }
        }
    }

    /**
     * 실제 중재 비즈니스 로직.
     * 기획서 요구사항에 맞게 교체/확장할 것.
     */
    protected String resolveConflict(String currentData, String version, String reporterId) {
        // 기본 구현: 현재 데이터를 그대로 확정 (Last-Write-Wins from Aether)
        // 실제로는 currentData를 파싱하여 게임 규칙에 맞는 연산 수행
        log.info(String.format(
            "[Aether] 중재 로직 실행 | version=%s | reporter=%s", version, reporterId));
        return currentData;
    }

    // ──────────────────────────────────────────────
    //  우아한 시스템 종료
    // ──────────────────────────────────────────────
    public void stop() {
        log.info("===== Aether 종료 시퀀스 시작 =====");
        running = false;

        // 1) HeartbeatMonitor 먼저 종료 (추가 재시작 방지)
        if (heartbeatMonitor != null) heartbeatMonitor.stop();

        // 2) 모든 Spark에 종료 명령 전파 (Redis Pub/Sub)
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(HeartbeatMonitor.CH_ORCHESTRATION, "cmd:shutdown:all");
            log.info("[Aether] 종료 명령 전파 완료.");
        } catch (Exception e) {
            log.warning("[Aether] 종료 명령 전파 실패: " + e.getMessage());
        }

        // 3) 각 Spark 프로세스 정상 종료 대기 (최대 15초)
        sparkMap.forEach((id, spark) -> {
            log.info("[Aether] " + id + " 종료 대기 중...");
            spark.stopGracefully(15);
        });
        sparkMap.clear();

        // 4) Redis 연결 풀 해제
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }

        log.info("===== Aether 종료 완료 =====");
    }

    // ──────────────────────────────────────────────
    //  내부 유틸
    // ──────────────────────────────────────────────
    private void initRedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, redisHost, redisPort, 3000);
        log.info("[Aether] Redis 연결 풀 초기화 완료: " + redisHost + ":" + redisPort);
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ──────────────────────────────────────────────
    //  Builder
    // ──────────────────────────────────────────────
    public static class Builder {
        private int    sparkCount = 3;
        private int    basePort   = 8100;
        private Path   jarPath;
        private String redisHost  = "localhost";
        private int    redisPort  = 6379;

        public Builder sparkCount(int n)       { this.sparkCount = n;    return this; }
        public Builder basePort(int p)         { this.basePort   = p;    return this; }
        public Builder jarPath(Path p)         { this.jarPath    = p;    return this; }
        public Builder redisHost(String h)     { this.redisHost  = h;    return this; }
        public Builder redisPort(int p)        { this.redisPort  = p;    return this; }

        public AetherOrchestrator build() {
            if (jarPath == null) throw new IllegalStateException("jarPath 설정 필요");
            return new AetherOrchestrator(this);
        }
    }
}
