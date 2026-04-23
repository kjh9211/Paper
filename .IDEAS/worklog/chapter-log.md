# Chapter 1 - 구조 설계 및 범위 확정
- `.ideas/개발 기획서.md`, `.ideas/설계의도.md`, Java 초안 4개를 분석했다.
- Paper 본체와 분리하기 위해 `aether-orchestrator` 독립 모듈 전략으로 확정했다.
- `settings.gradle.kts`에 `optionalInclude("aether-orchestrator")`를 추가했다.
- `aether-orchestrator.settings.gradle.kts`와 기본 디렉터리 구조를 생성했다.

# Chapter 2 - 핵심 구현
- `AetherOrchestrator`, `HeartbeatMonitor`, `SparkProcess`, `SparkHeartbeatPublisher`를 구현했다.
- 시작/종료 수명주기, Redis heartbeat 감시, dead spark 재기동, conflict 이벤트 중재 흐름을 반영했다.
- 실행 엔트리포인트 `AetherLauncher`를 추가해 config/lang 기반 시작이 가능하게 했다.

# Chapter 3 - 설정 파일 분리
- `config.yml`을 추가하고 Redis, orchestrator, DB(H2/MySQL) 기본값을 분리했다.
- `lang/ko.yml`을 추가하고 런처 메시지를 분리했다.
- 빌드 의존성에 `jedis`, `snakeyaml`, `h2`, `mysql-connector-j`를 반영했다.

# Chapter 4 - 검증 계획
- `:aether-orchestrator:compileJava` 실행으로 컴파일 검증을 수행한다.
- 실행 예시: `./gradlew :aether-orchestrator:run` (spark JAR 경로 필요).

## Chapter 4 실행 결과
- `:aether-orchestrator:compileJava` 성공 (BUILD SUCCESSFUL).
- 검증 시 로컬 기본 JVM이 1.8이라 실패하여, IntelliJ JBR(17+)로 JAVA_HOME을 지정해 해결했다.
- Java 소스 파일의 UTF-8 BOM 이슈를 제거해 컴파일 오류를 정리했다.

# Chapter 5 - 인스턴스/접속 요구사항 반영
- 각 Spark 인스턴스가 개별 작업 디렉터리(`instances/{sparkId}`)에서 실행되도록 변경했다.
- `sharedPluginsDir`의 `.jar` 파일만 인스턴스 `plugins/`로 동기화하도록 구현했다.
- 최초 실행 시 `eula.txt`를 자동 생성하고 `eula=true`를 기록하도록 구현했다.
- 접속 모드를 `DIRECT`, `VELOCITY`, `DUAL`로 추가하고, `server.properties`와 `paper-global.yml`을 모드에 맞게 생성하도록 반영했다.
- `config.yml`에 `instancesRootDir`, `sharedPluginsDir`, `network.mode`, `network.velocitySecret` 설정을 추가했다.

# Chapter 6 - 영역 간 유저 이동 동기화
- `UserMigrationService`를 추가해 Redis 공유키(`nexus:user:{userId}`) + 이동 채널(`channel:user-move`) 기반 전송을 구현했다.
- 대상 Spark 전용 구독 스레드가 이동 신호를 받으면 Redis에서 최신 유저 데이터를 읽고 로컬 저장소를 즉시 덮어쓰도록 했다.
- `SparkHeartbeatPublisher`에 이동 서비스 시작/종료를 연결했다.
- `channel:orchestration`의 `cmd:move-user:{userId}:{targetSparkId}:{json}` 명령을 받아 이동 신호를 발행하도록 추가했다.
