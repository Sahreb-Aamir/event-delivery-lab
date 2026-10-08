package dev.sahreb.delivery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.server.KafkaRaftServer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.common.MetadataVersion;

/** One temporary, loopback-only Kafka node for the local development launcher. */
final class LocalKafkaBroker implements AutoCloseable {
    private final KafkaRaftServer server;
    private final Path directory;
    private final int brokerPort;
    private final int controllerPort;
    private final AtomicBoolean closed = new AtomicBoolean();

    private LocalKafkaBroker(KafkaRaftServer server, Path directory, int brokerPort, int controllerPort) {
        this.server = server;
        this.directory = directory;
        this.brokerPort = brokerPort;
        this.controllerPort = controllerPort;
    }

    static LocalKafkaBroker start() throws Exception {
        Path directory = Files.createTempDirectory("event-delivery-lab-kafka-").toAbsolutePath().normalize();
        LocalKafkaBroker broker = null;
        try {
            // Kafka's static single-controller quorum needs a concrete port. Reserve
            // both ephemeral ports until immediately before Kafka opens its sockets.
            try (ServerSocket brokerReservation = reservePort(); ServerSocket controllerReservation = reservePort()) {
                int brokerPort = brokerReservation.getLocalPort();
                int controllerPort = controllerReservation.getLocalPort();
                Properties properties = new Properties();
                properties.setProperty("process.roles", "broker,controller");
                properties.setProperty("node.id", "1");
                properties.setProperty("listeners", "PLAINTEXT://127.0.0.1:" + brokerPort
                        + ",CONTROLLER://127.0.0.1:" + controllerPort);
                properties.setProperty("advertised.listeners", "PLAINTEXT://127.0.0.1:" + brokerPort);
                properties.setProperty("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");
                properties.setProperty("inter.broker.listener.name", "PLAINTEXT");
                properties.setProperty("controller.listener.names", "CONTROLLER");
                properties.setProperty("controller.quorum.voters", "1@127.0.0.1:" + controllerPort);
                properties.setProperty("log.dirs", directory.toString());
                properties.setProperty("num.partitions", "1");
                properties.setProperty("offsets.topic.num.partitions", "1");
                properties.setProperty("offsets.topic.replication.factor", "1");
                properties.setProperty("transaction.state.log.replication.factor", "1");
                properties.setProperty("transaction.state.log.min.isr", "1");
                properties.setProperty("group.initial.rebalance.delay.ms", "0");
                properties.setProperty("log.cleaner.dedupe.buffer.size", "2097152");
                properties.setProperty("server.max.startup.time.ms", "60000");

                new Formatter().setNodeId(1).setClusterId(Uuid.randomUuid().toString())
                        .setDirectories(List.of(directory.toString()))
                        .setControllerListenerName("CONTROLLER")
                        .setReleaseVersion(MetadataVersion.latestProduction())
                        .setHasDynamicQuorum(false).run();
                KafkaRaftServer server = new KafkaRaftServer(kafka.server.KafkaConfig.fromProps(properties), Time.SYSTEM);
                broker = new LocalKafkaBroker(server, directory, brokerPort, controllerPort);
            }
            broker.server.startup();
            return broker;
        } catch (Exception | Error failure) {
            try {
                if (broker != null) {
                    broker.close();
                } else {
                    deleteOwnedDirectory(directory);
                }
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    String bootstrapServers() {
        return "127.0.0.1:" + brokerPort;
    }

    List<Integer> listenerPorts() {
        return List.of(brokerPort, controllerPort);
    }

    Path storageDirectory() {
        return directory;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            server.shutdown();
            server.awaitShutdown();
            try {
                deleteOwnedDirectory(directory);
            } catch (IOException failure) {
                throw new IllegalStateException("Could not remove the temporary Kafka directory " + directory, failure);
            }
        }
    }

    private static ServerSocket reservePort() throws IOException {
        ServerSocket socket = new ServerSocket();
        try {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket;
        } catch (IOException failure) {
            socket.close();
            throw failure;
        }
    }

    private static void deleteOwnedDirectory(Path directory) throws IOException {
        Path temporaryRoot = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
        if (!directory.startsWith(temporaryRoot) || !directory.getFileName().toString().startsWith("event-delivery-lab-kafka-")) {
            throw new IOException("Refusing to remove a directory outside the launcher's temporary storage");
        }
        if (Files.exists(directory)) {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }
}
