package com.aether.orchestrator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SparkProcess {
    private static final Logger log = Logger.getLogger(SparkProcess.class.getName());

    public enum SparkStatus { STOPPED, STARTING, RUNNING, RESTARTING, DEAD }

    private final String sparkId;
    private final int port;
    private final Path jarPath;
    private final String redisHost;
    private final int redisPort;
    private final Path instancesRootDir;
    private final Path sharedPluginsDir;
    private final ConnectionMode connectionMode;
    private final String velocitySecret;

    private Process process;
    private long pid = -1;
    private Instant startedAt;
    private SparkStatus status = SparkStatus.STOPPED;
    private Path instanceDir;

    public enum ConnectionMode {
        DIRECT,
        VELOCITY,
        DUAL;

        public static ConnectionMode from(String raw) {
            if (raw == null || raw.isBlank()) {
                return DIRECT;
            }
            try {
                return ConnectionMode.valueOf(raw.trim().toUpperCase());
            } catch (IllegalArgumentException ignored) {
                return DIRECT;
            }
        }
    }

    public SparkProcess(
        String sparkId,
        int port,
        Path jarPath,
        String redisHost,
        int redisPort,
        Path instancesRootDir,
        Path sharedPluginsDir,
        ConnectionMode connectionMode,
        String velocitySecret
    ) {
        this.sparkId = sparkId;
        this.port = port;
        this.jarPath = jarPath;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.instancesRootDir = instancesRootDir;
        this.sharedPluginsDir = sharedPluginsDir;
        this.connectionMode = connectionMode;
        this.velocitySecret = velocitySecret;
    }

    public void start() throws IOException {
        if (isAlive()) {
            return;
        }
        this.instanceDir = prepareInstanceDirectory();

        ProcessBuilder builder = new ProcessBuilder(List.of("java", "-Xmx512m", "-jar", jarPath.toAbsolutePath().toString(), "nogui"));
        builder.directory(instanceDir.toFile());
        builder.environment().put("SPARK_ID", sparkId);
        builder.environment().put("SPARK_PORT", String.valueOf(port));
        builder.environment().put("REDIS_HOST", redisHost);
        builder.environment().put("REDIS_PORT", String.valueOf(redisPort));
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        this.process = builder.start();
        this.pid = this.process.pid();
        this.startedAt = Instant.now();
        this.status = SparkStatus.STARTING;
        log.info("spark started: " + sparkId + " pid=" + pid + " port=" + port);
    }

    private Path prepareInstanceDirectory() throws IOException {
        Path rootDir = this.instancesRootDir.toAbsolutePath().normalize();
        Path dir = rootDir.resolve(sparkId).normalize();

        if (!dir.startsWith(rootDir)) {
            throw new IOException("Invalid instance path: " + dir);
        }

        Files.createDirectories(dir);
        Files.createDirectories(dir.resolve("plugins"));
        syncPluginJars(dir.resolve("plugins"));
        ensureEulaAccepted(dir);
        writeServerProperties(dir);
        writePaperGlobalConfig(dir);
        return dir;
    }

    private void syncPluginJars(Path instancePluginsDir) throws IOException {
        Path sourceDir = sharedPluginsDir.toAbsolutePath().normalize();
        if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
            return;
        }
        try (var stream = Files.list(sourceDir)) {
            stream
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".jar"))
                .forEach(path -> {
                    try {
                        Files.copy(path, instancePluginsDir.resolve(path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new RuntimeException("plugin jar copy failed: " + path, e);
                    }
                });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioe) {
                throw ioe;
            }
            throw e;
        }
    }

    private void ensureEulaAccepted(Path dir) throws IOException {
        Path eulaFile = dir.resolve("eula.txt");
        if (!Files.exists(eulaFile)) {
            Files.writeString(
                eulaFile,
                "# Auto-generated by Aether" + System.lineSeparator() + "eula=true" + System.lineSeparator(),
                StandardCharsets.UTF_8
            );
        }
    }

    private void writeServerProperties(Path dir) throws IOException {
        Path propsPath = dir.resolve("server.properties");
        Properties props = new Properties();

        if (Files.exists(propsPath)) {
            try (InputStream in = Files.newInputStream(propsPath)) {
                props.load(in);
            }
        }

        props.setProperty("server-port", String.valueOf(port));
        props.setProperty("server-ip", "0.0.0.0");
        props.setProperty("motd", "Aether " + sparkId);

        boolean onlineMode = connectionMode == ConnectionMode.DIRECT;
        props.setProperty("online-mode", String.valueOf(onlineMode));

        try (OutputStream out = Files.newOutputStream(propsPath)) {
            props.store(out, "Managed by Aether");
        }
    }

    private void writePaperGlobalConfig(Path dir) throws IOException {
        Path configDir = dir.resolve("config");
        Files.createDirectories(configDir);
        Path paperGlobal = configDir.resolve("paper-global.yml");

        Map<String, Object> root = new HashMap<>();
        Map<String, Object> proxies = new HashMap<>();
        Map<String, Object> velocity = new HashMap<>();

        switch (connectionMode) {
            case DIRECT -> {
                velocity.put("enabled", false);
                velocity.put("online-mode", false);
                velocity.put("secret", "");
            }
            case VELOCITY -> {
                velocity.put("enabled", true);
                velocity.put("online-mode", true);
                velocity.put("secret", velocitySecret);
            }
            case DUAL -> {
                velocity.put("enabled", false);
                velocity.put("online-mode", false);
                velocity.put("secret", "");
            }
        }

        proxies.put("velocity", velocity);
        root.put("proxies", proxies);

        String yaml = new org.yaml.snakeyaml.Yaml().dump(root);
        Files.writeString(paperGlobal, yaml, StandardCharsets.UTF_8);
    }

    public boolean isAlive() {
        return this.process != null && this.process.isAlive();
    }

    public boolean stopGracefully(long timeoutSeconds) {
        if (!isAlive()) {
            return true;
        }
        this.process.destroy();
        try {
            boolean exited = this.process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!exited) {
                this.process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.process.destroyForcibly();
        }
        this.status = SparkStatus.STOPPED;
        return !isAlive();
    }

    public String getSparkId() {
        return sparkId;
    }

    public long getPid() {
        return pid;
    }

    public int getPort() {
        return port;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public SparkStatus getStatus() {
        return status;
    }

    public void setStatus(SparkStatus status) {
        this.status = status;
    }
}
