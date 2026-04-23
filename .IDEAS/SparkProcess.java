package com.aether.orchestrator;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Spark 인스턴스 하나의 OS 프로세스를 래핑하는 클래스.
 * 외부 JAR 실행, PID 추적, 생존 확인, 강제/정상 종료를 담당한다.
 */
public class SparkProcess {

    private static final Logger log = Logger.getLogger(SparkProcess.class.getName());

    private final String sparkId;          // e.g. "spark-1"
    private final int    port;             // Spark가 리슨할 포트
    private final Path   jarPath;          // 실행할 JAR 경로
    private final String redisHost;
    private final int    redisPort;

    private Process   process;
    private long      pid = -1;
    private Instant   startedAt;
    private SparkStatus status = SparkStatus.STOPPED;

    public enum SparkStatus { STOPPED, STARTING, RUNNING, RESTARTING, DEAD }

    // ──────────────────────────────────────────────
    //  생성자
    // ──────────────────────────────────────────────
    public SparkProcess(String sparkId, int port, Path jarPath,
                        String redisHost, int redisPort) {
        this.sparkId   = sparkId;
        this.port      = port;
        this.jarPath   = jarPath;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    // ──────────────────────────────────────────────
    //  기동
    // ──────────────────────────────────────────────
    /**
     * JAR를 자식 프로세스로 실행한다.
     * Spark JAR은 아래 환경 변수를 읽어 자신을 초기화한다:
     *   SPARK_ID, SPARK_PORT, REDIS_HOST, REDIS_PORT
     */
    public void start() throws IOException {
        if (isAlive()) {
            log.warning("[" + sparkId + "] 이미 실행 중. 재기동 무시.");
            return;
        }

        List<String> cmd = List.of(
            "java",
            "-Xmx512m",
            "-jar", jarPath.toAbsolutePath().toString()
        );

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("SPARK_ID",    sparkId);
        pb.environment().put("SPARK_PORT",  String.valueOf(port));
        pb.environment().put("REDIS_HOST",  redisHost);
        pb.environment().put("REDIS_PORT",  String.valueOf(redisPort));

        // 자식 프로세스의 stdout/stderr를 부모(Aether) 로그로 리다이렉트
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        process   = pb.start();
        pid       = process.pid();          // Java 9+: ProcessHandle API
        startedAt = Instant.now();
        status    = SparkStatus.STARTING;

        log.info(String.format("[%s] 기동 완료 | PID=%d | PORT=%d", sparkId, pid, port));
    }

    // ──────────────────────────────────────────────
    //  상태 조회
    // ──────────────────────────────────────────────
    public boolean isAlive() {
        return process != null && process.isAlive();
    }

    /**
     * OS 레벨에서 PID가 살아 있는지 이중으로 확인한다.
     * ProcessHandle은 process.isAlive()보다 신뢰도가 높다.
     */
    public boolean isAliveByHandle() {
        if (pid < 0) return false;
        return ProcessHandle.of(pid)
                            .map(ProcessHandle::isAlive)
                            .orElse(false);
    }

    // ──────────────────────────────────────────────
    //  종료
    // ──────────────────────────────────────────────
    /**
     * 우아한 종료: SIGTERM → 10초 대기 → SIGKILL
     */
    public boolean stopGracefully(long timeoutSeconds) {
        if (!isAlive()) return true;

        log.info("[" + sparkId + "] 정상 종료 신호(SIGTERM) 전송...");
        process.destroy();                          // SIGTERM

        try {
            boolean exited = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!exited) {
                log.warning("[" + sparkId + "] 타임아웃. 강제 종료(SIGKILL) 실행.");
                process.destroyForcibly();          // SIGKILL
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }

        status = SparkStatus.STOPPED;
        log.info("[" + sparkId + "] 종료 완료.");
        return !isAlive();
    }

    // ──────────────────────────────────────────────
    //  Getters
    // ──────────────────────────────────────────────
    public String      getSparkId()  { return sparkId; }
    public long        getPid()      { return pid; }
    public int         getPort()     { return port; }
    public Instant     getStartedAt(){ return startedAt; }
    public SparkStatus getStatus()   { return status; }
    public void        setStatus(SparkStatus s) { this.status = s; }
}
