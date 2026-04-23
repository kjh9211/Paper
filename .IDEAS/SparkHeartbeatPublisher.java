package com.aether.spark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Spark(자식 서버) 측에서 실행되는 Heartbeat 발행기.
 *
 * Aether의 HeartbeatMonitor와 대응하는 클라이언트 구현체.
 * 환경 변수에서 자신의 ID/포트를 읽어 주기적으로 Redis에 기록한다.
 *
 * Redis 기록 내용:
 *   HSET spark:registry:{SPARK_ID}  heartbeat_ts <unix_epoch>
 *   PUBLISH channel:heartbeat        "{SPARK_ID}:alive"
 */
public class SparkHeartbeatPublisher {

    private static final Logger log = Logger.getLogger(SparkHeartbeatPublisher.class.getName());

    private static final int HEARTBEAT_INTERVAL_SEC = 5;

    private final String   sparkId;
    private final JedisPool jedisPool;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "spark-heartbeat"));

    public SparkHeartbeatPublisher(String sparkId, JedisPool jedisPool) {
        this.sparkId  = sparkId;
        this.jedisPool = jedisPool;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
            this::publishHeartbeat,
            0,
            HEARTBEAT_INTERVAL_SEC,
            TimeUnit.SECONDS
        );
        log.info("[" + sparkId + "] HeartbeatPublisher 시작.");
    }

    private void publishHeartbeat() {
        try (Jedis jedis = jedisPool.getResource()) {
            long now = Instant.now().getEpochSecond();

            // 1) Registry Hash 갱신
            jedis.hset("spark:registry:" + sparkId, Map.of(
                "heartbeat_ts", String.valueOf(now),
                "pid",          String.valueOf(ProcessHandle.current().pid())
            ));

            // 2) 실시간 Pub/Sub 신호 발행
            jedis.publish("channel:heartbeat", sparkId + ":alive");

        } catch (Exception e) {
            log.warning("[" + sparkId + "] Heartbeat 발행 실패: " + e.getMessage());
        }
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // ──────────────────────────────────────────────
    //  Spark 메인 (참고용 엔트리포인트)
    // ──────────────────────────────────────────────
    public static void main(String[] args) {
        // Aether가 환경 변수로 주입한 값들을 읽음
        String sparkId   = System.getenv().getOrDefault("SPARK_ID",   "spark-dev");
        int    port      = Integer.parseInt(System.getenv().getOrDefault("SPARK_PORT",  "8101"));
        String redisHost = System.getenv().getOrDefault("REDIS_HOST",  "localhost");
        int    redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT",  "6379"));

        log.info(String.format("[%s] 기동 | PORT=%d | Redis=%s:%d",
            sparkId, port, redisHost, redisPort));

        JedisPool pool = new JedisPool(redisHost, redisPort);

        SparkHeartbeatPublisher publisher = new SparkHeartbeatPublisher(sparkId, pool);
        publisher.start();

        // Orchestration 채널 구독: Aether 명령 수신
        try (Jedis jedis = pool.getResource()) {
            jedis.subscribe(new redis.clients.jedis.JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    if ("cmd:shutdown:all".equals(message) ||
                        ("cmd:shutdown:" + sparkId).equals(message)) {

                        log.info("[" + sparkId + "] 종료 명령 수신. 정리 시작...");
                        publisher.stop();
                        pool.close();
                        unsubscribe(); // subscribe 루프 탈출
                    }
                    if (message.startsWith("cmd:sync:")) {
                        String key = message.substring("cmd:sync:".length());
                        log.info("[" + sparkId + "] 동기화 신호 수신: " + key);
                        // TODO: 해당 키의 Redis 데이터를 로컬 캐시에 반영
                    }
                }
            }, "channel:orchestration");
        }

        log.info("[" + sparkId + "] 종료 완료.");
    }
}
