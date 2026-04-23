package com.aether.orchestrator;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public final class AetherLauncher {
    private static final Logger log = Logger.getLogger(AetherLauncher.class.getName());

    private AetherLauncher() {}

    public static void main(String[] args) throws InterruptedException {
        AppConfig config = AppConfig.load();
        Messages messages = Messages.load(config.lang());

        log.info(messages.get("app.starting"));
        log.info("database.type=" + config.database().type());

        AetherOrchestrator orchestrator = new AetherOrchestrator.Builder()
            .sparkCount(config.orchestrator().sparkCount())
            .basePort(config.orchestrator().basePort())
            .jarPath(Path.of(config.orchestrator().sparkJarPath()))
            .redisHost(config.redis().host())
            .redisPort(config.redis().port())
            .restartCooldownSeconds(config.orchestrator().restartCooldownSeconds())
            .instancesRootDir(Path.of(config.orchestrator().instancesRootDir()))
            .sharedPluginsDir(Path.of(config.orchestrator().sharedPluginsDir()))
            .connectionMode(config.network().mode())
            .velocitySecret(config.network().velocitySecret())
            .build();

        orchestrator.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info(messages.get("app.stopping"));
            orchestrator.stop();
        }));

        new CountDownLatch(1).await();
    }
}
