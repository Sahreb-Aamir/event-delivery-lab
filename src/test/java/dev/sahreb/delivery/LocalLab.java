package dev.sahreb.delivery;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Development-only entry point: broker libraries never enter the application jar. */
public final class LocalLab {
    private LocalLab() { }

    public static void main(String[] args) throws Exception {
        var broker = LocalKafkaBroker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "local-kafka-shutdown"));
        var application = new SpringApplication(EventDeliveryLabApplication.class);
        application.addInitializers(context -> context.getEnvironment().getPropertySources().addFirst(
                new MapPropertySource("localLabConsumerLifecycle",
                        Map.of("spring.kafka.listener.auto-startup", false,
                                "spring.kafka.bootstrap-servers", broker.bootstrapServers()))));
        ConfigurableApplicationContext context = null;
        try {
            context = application.run(args);
            context.getBean(DeliveryStore.class).interruptUnfinished();
            context.getBean(KafkaListenerEndpointRegistry.class).start();
        } catch (RuntimeException | Error startupFailure) {
            if (context != null) {
                context.close();
            }
            broker.close();
            throw startupFailure;
        }
    }
}
