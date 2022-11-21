package be.kuleuven.distributedsystems.cloud;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.hateoas.config.EnableHypermediaSupport;
import org.springframework.hateoas.config.HypermediaWebClientConfigurer;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.util.Objects;

@EnableHypermediaSupport(type = EnableHypermediaSupport.HypermediaType.HAL)
@SpringBootApplication
public class Application {
    private static final String hostPortPubSub = "localhost:8083";
    private static final String TOPIC = "confirmQuotes";
    private static final String PROJECT_ID = "demo-distributed-systems-kul";
    static String hostPortFireStore = "localhost:8084";
    private static boolean subCreated = true;
    private static final ManagedChannel pubSubChannel = ManagedChannelBuilder.forTarget(hostPortPubSub).usePlaintext().build();

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        System.setProperty("server.port", System.getenv().getOrDefault("PORT", "8080"));

        ApplicationContext context = SpringApplication.run(Application.class, args);
        createTopic();

        // TODO: (level 2) load this data into Firestore
        String data = new String(new ClassPathResource("data.json").getInputStream().readAllBytes());
    }

    @Bean
    public boolean isProduction() {
        return Objects.equals(System.getenv("GAE_ENV"), "standard");
    }

    @Bean
    public static String projectId() {
        return PROJECT_ID;
    }
    @Bean
    public static String topicId() {
        return TOPIC;
    }
    /*
     * You can use this builder to create a Spring WebClient instance which can be used to make REST-calls.
     */
    @Bean
    WebClient.Builder webClientBuilder(HypermediaWebClientConfigurer configurer) {
        return configurer.registerHypermediaTypes(WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
                .codecs(clientCodecConfigurer -> clientCodecConfigurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024)));
    }

    private static void createTopic() throws IOException {
        try(TopicAdminClient topicAdminClient =
                    TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(pubSubTransportChannelProvider())
                            .setCredentialsProvider(pubSubCredentialsProvider())
                            .build());
        ) {
            TopicName topicName = TopicName.of(projectId(), topicId());
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: "+ topic.getName());
        } catch (Exception e) {
            System.out.println("did not create topic");
        }
        initSub();
    }
    private static void initSub() throws IOException {
        PushConfig pushConfig = PushConfig.newBuilder()
                .setPushEndpoint("http://localhost:8080/pubsub/subs")
                .build();
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
                SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(pubSubTransportChannelProvider())
                        .setCredentialsProvider(pubSubCredentialsProvider())
                        .build());
        try {
            subscriptionAdminClient.createSubscription(
                    SubscriptionName.of(projectId(), "subs"),
                    TopicName.of(projectId(), topicId()),
                    pushConfig, 60);
        } catch (Exception e) {
            System.out.println("Sub is already created.");
        }

    }



    private static String getSubName() {
        return "confirmQuotes";
    }

    @Bean
    public static TransportChannelProvider pubSubTransportChannelProvider() {
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(pubSubChannel));
    }

    @Bean
    public static CredentialsProvider pubSubCredentialsProvider() {
        return NoCredentialsProvider.create();
    }
    @Bean
    public static Firestore db() {
        return FirestoreOptions.getDefaultInstance()
                .toBuilder()
                .setProjectId(projectId())
                .setChannelProvider(fireStoreTransportChannelProvider())
                .setCredentials(new FirestoreOptions.EmulatorCredentials())
                .build()
                .getService();
    }

    private static TransportChannelProvider fireStoreTransportChannelProvider() {

        return InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(hostPortFireStore)
                .setChannelConfigurator((ManagedChannelBuilder::usePlaintext))
                .build();
    }

    @Bean
    HttpFirewall httpFirewall() {
        DefaultHttpFirewall firewall = new DefaultHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        return firewall;
    }
}
