package dev.sahreb.delivery;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LocalKafkaBrokerTest {
    @Test
    void startsOnlyOnLoopbackServesKafkaAndCleansUpItsStorage() throws Exception {
        Path directory;
        List<Integer> ports;
        try (LocalKafkaBroker broker = LocalKafkaBroker.start()) {
            directory = broker.storageDirectory();
            ports = broker.listenerPorts();
            assertThat(directory).isDirectory();
            assertThat(ports).hasSize(2).doesNotHaveDuplicates().allMatch(port -> port > 0);
            for (int port : ports) {
                try (Socket connection = new Socket()) {
                    connection.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
                }
            }
            // A wildcard listener would prevent a second bind on any non-loopback
            // local interface. Exercise actual sockets, not just config strings.
            try (var interfaces = NetworkInterface.networkInterfaces()) {
                for (NetworkInterface network : interfaces.toList()) {
                    if (network.isUp() && !network.isLoopback()) {
                        for (InetAddress address : network.inetAddresses().toList()) {
                            if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                                for (int port : ports) {
                                    try (ServerSocket probe = new ServerSocket()) {
                                        probe.bind(new InetSocketAddress(address, port));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker.bootstrapServers(),
                    "request.timeout.ms", "5000", "default.api.timeout.ms", "10000"))) {
                assertThat(admin.describeCluster().nodes().get(10, TimeUnit.SECONDS)).singleElement()
                        .satisfies(node -> {
                            assertThat(node.host()).isEqualTo("127.0.0.1");
                            assertThat(node.port()).isEqualTo(ports.get(0));
                        });
            }
        }
        assertThat(directory).doesNotExist();
        for (int port : ports) {
            try (ServerSocket probe = new ServerSocket()) {
                probe.bind(new InetSocketAddress("127.0.0.1", port));
            }
        }
    }
}
