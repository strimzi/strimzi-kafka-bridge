/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.kafka.bridge.ConnectionEndpoint;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.HealthCheckable;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Main bridge class listening for connections
 * and handling AMQP senders and receivers
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class AmqpBridge extends AbstractVerticle implements HealthCheckable {

    private static final Logger log = LoggerFactory.getLogger(AmqpBridge.class);

    // AMQP message annotations
    public static final String AMQP_PARTITION_ANNOTATION = "x-opt-bridge.partition";
    public static final String AMQP_KEY_ANNOTATION = "x-opt-bridge.key";
    public static final String AMQP_OFFSET_ANNOTATION = "x-opt-bridge.offset";
    public static final String AMQP_TOPIC_ANNOTATION = "x-opt-bridge.topic";

    // AMQP errors
    public static final String AMQP_ERROR_NO_PARTITIONS = "io.strimzi:no-free-partitions";
    public static final String AMQP_ERROR_NO_GROUPID = "io.strimzi:no-group-id";
    public static final String AMQP_ERROR_PARTITION_NOT_EXISTS = "io.strimzi:partition-not-exists";
    public static final String AMQP_ERROR_SEND_TO_KAFKA = "io.strimzi:error-to-kafka";
    public static final String AMQP_ERROR_WRONG_PARTITION_FILTER = "io.strimzi:wrong-partition-filter";
    public static final String AMQP_ERROR_WRONG_OFFSET_FILTER = "io.strimzi:wrong-offset-filter";
    public static final String AMQP_ERROR_NO_PARTITION_FILTER = "io.strimzi:no-partition-filter";
    public static final String AMQP_ERROR_WRONG_FILTER = "io.strimzi:wrong-filter";
    public static final String AMQP_ERROR_KAFKA_SUBSCRIBE = "io.strimzi:kafka-subscribe";
    public static final String AMQP_ERROR_KAFKA_COMMIT = "io.strimzi:kafka-commit";
    public static final String AMQP_ERROR_CONFIGURATION = "io.strimzi:configuration";

    // AMQP filters
    public static final String AMQP_PARTITION_FILTER = "io.strimzi:partition-filter:int";
    public static final String AMQP_OFFSET_FILTER = "io.strimzi:offset-filter:long";

    private final BridgeConfig bridgeConfig;

    // container-id needed for working in "client" mode
    private static final String CONTAINER_ID = "kafka-bridge-service";

    private static final int HEALTH_SERVER_PORT = 8080;

    // AMQP client/server related stuff
    private ProtonServer server;
    private ProtonClient client;

    // endpoints for handling incoming and outcoming messages
    private Map<ProtonConnection, ConnectionEndpoint> endpoints;

    // if the bridge is ready to handle requests
    private boolean isReady = false;

    private MeterRegistry meterRegistry;

    /**
     * Constructor
     *
     * @param bridgeConfig bridge configuration
     * @param meterRegistry registry for scraping metrics
     */
    public AmqpBridge(BridgeConfig bridgeConfig, MeterRegistry meterRegistry) {
        this.bridgeConfig = bridgeConfig;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Start the AMQP server
     *
     * @param startPromise
     */
    private void bindAmqpServer(Promise<Void> startPromise) {

        ProtonServerOptions options = this.createServerOptions();

        this.server = ProtonServer.create(this.vertx, options)
                .connectHandler(this::processConnection)
                .listen(ar -> {

                    if (ar.succeeded()) {

                        log.info("AMQP-Kafka Bridge started and listening on port {}", ar.result().actualPort());
                        log.info("AMQP-Kafka Bridge bootstrap servers {}",
                                this.bridgeConfig.getKafkaConfig().getConfig()
                                        .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                        );

                        this.isReady = true;

                        startPromise.complete();
                    } else {
                        log.error("Error starting AMQP-Kafka Bridge", ar.cause());
                        startPromise.fail(ar.cause());
                    }
                });
    }

    /**
     * Connect to an AMQP server/router
     *
     * @param startPromise
     */
    private void connectAmqpClient(Promise<Void> startPromise) {

        this.client = ProtonClient.create(this.vertx);

        String host = this.bridgeConfig.getAmqpConfig().getHost();
        int port = this.bridgeConfig.getAmqpConfig().getPort();

        ProtonClientOptions options = this.createClientOptions();

        this.client.connect(options, host, port, ar -> {

            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.setContainer(CONTAINER_ID);

                this.processConnection(connection);

                log.info("AMQP-Kafka Bridge started and connected in client mode to {}:{}", host, port);
                log.info("AMQP-Kafka Bridge bootstrap servers {}",
                        this.bridgeConfig.getKafkaConfig().getConfig()
                                .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                );

                this.isReady = true;

                startPromise.complete();

            } else {
                log.error("Error connecting AMQP-Kafka Bridge as client", ar.cause());
                startPromise.fail(ar.cause());
            }
        });
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        log.info("Starting AMQP-Kafka bridge verticle...");

        this.endpoints = new HashMap<>();

        AmqpMode mode = this.bridgeConfig.getAmqpConfig().getMode();
        log.info("AMQP-Kafka Bridge configured in {} mode", mode);
        if (mode == AmqpMode.SERVER) {
            this.bindAmqpServer(startPromise);
        } else {
            this.connectAmqpClient(startPromise);
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

        log.info("Stopping AMQP-Kafka bridge verticle ...");

        this.isReady = false;

        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.endpoints.forEach((connection, endpoint) -> {

            if (endpoint.getSource() != null) {
                endpoint.getSource().close();
            }
            if (!endpoint.getSinks().isEmpty()) {
                endpoint.getSinks().stream().forEach(sink -> sink.close());
            }
            connection.close();
        });
        this.endpoints.clear();

        if (this.server != null) {

            this.server.close(done -> {

                if (done.succeeded()) {
                    log.info("AMQP-Kafka bridge has been shut down successfully");
                    stopPromise.complete();
                } else {
                    log.info("Error while shutting down AMQP-Kafka bridge", done.cause());
                    stopPromise.fail(done.cause());
                }
            });
        }
    }

    /**
     * Create an options instance for the ProtonServer
     * based on AMQP-Kafka bridge internal configuration
     *
     * @return ProtonServer options instance
     */
    private ProtonServerOptions createServerOptions() {

        ProtonServerOptions options = new ProtonServerOptions();
        options.setHost(this.bridgeConfig.getAmqpConfig().getHost());
        options.setPort(this.bridgeConfig.getAmqpConfig().getPort());

        if (this.bridgeConfig.getAmqpConfig().getCertDir() != null && this.bridgeConfig.getAmqpConfig().getCertDir().length() > 0) {
            String certDir = this.bridgeConfig.getAmqpConfig().getCertDir();
            log.info("Enabling SSL configuration for AMQP with TLS certificates from {}", certDir);
            options.setSsl(true)
                    .setPemTrustOptions(new PemTrustOptions()
                            .addCertPath(new File(certDir, "ca.crt").getAbsolutePath()))
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                            .addCertPath(new File(certDir, "tls.crt").getAbsolutePath())
                            .addKeyPath(new File(certDir, "tls.key").getAbsolutePath()));
        }

        return options;
    }

    /**
     * Create an options instance for the ProtonClient
     *
     * @return ProtonClient options instance
     */
    private ProtonClientOptions createClientOptions() {

        ProtonClientOptions options = new ProtonClientOptions();
        options.setConnectTimeout(1000);
        options.setReconnectAttempts(-1).setReconnectInterval(1000); // reconnect forever, every 1000 millisecs

        if (this.bridgeConfig.getAmqpConfig().getCertDir() != null && this.bridgeConfig.getAmqpConfig().getCertDir().length() > 0) {
            String certDir = this.bridgeConfig.getAmqpConfig().getCertDir();
            log.info("Enabling SSL configuration for AMQP with TLS certificates from {}", certDir);
            options.setSsl(true)
                    .addEnabledSaslMechanism("EXTERNAL")
                    .setHostnameVerificationAlgorithm("")
                    .setPemTrustOptions(new PemTrustOptions()
                            .addCertPath(new File(certDir, "ca.crt").getAbsolutePath()))
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                            .addCertPath(new File(certDir, "tls.crt").getAbsolutePath())
                            .addKeyPath(new File(certDir, "tls.key").getAbsolutePath()));
        }

        return options;
    }

    /**
     * Process a connection request accepted by the Proton server or
     * open the connection if it's working as client
     *
     * @param connection Proton connection accepted instance
     */
    private void processConnection(ProtonConnection connection) {

        connection
                .openHandler(this::processOpenConnection)
                .closeHandler(this::processCloseConnection)
                .disconnectHandler(this::processDisconnection)
                .sessionOpenHandler(this::processOpenSession)
                .receiverOpenHandler(receiver -> {
                    this.processOpenReceiver(connection, receiver);
                })
                .senderOpenHandler(sender -> {
                    this.processOpenSender(connection, sender);
                });

        if (this.bridgeConfig.getAmqpConfig().getMode() == AmqpMode.CLIENT) {
            connection.open();
        }
    }

    /**
     * Handler for connection opened by remote
     *
     * @param ar async result with info on related Proton connection
     */
    private void processOpenConnection(AsyncResult<ProtonConnection> ar) {

        if (ar.succeeded()) {

            log.info("Connection opened by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());

            ProtonConnection connection = ar.result();
            connection.open();

            // new connection, preparing for hosting related sink/source endpoints
            if (!this.endpoints.containsKey(connection)) {
                this.endpoints.put(connection, new ConnectionEndpoint());
            }
        }
    }

    /**
     * Handler for connection closed by remote
     *
     * @param ar async result with info on related Proton connection
     */
    private void processCloseConnection(AsyncResult<ProtonConnection> ar) {

        if (ar.succeeded()) {
            log.info("Connection closed by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
            this.closeConnectionEndpoint(ar.result());
        }
    }

    /**
     * Handler for disconnection from the remote
     *
     * @param connection related Proton connection closed
     */
    private void processDisconnection(ProtonConnection connection) {

        log.info("Disconnection from {} {}", connection.getRemoteHostname(), connection.getRemoteContainer());
        this.closeConnectionEndpoint(connection);
    }

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection connection for which closing related endpoint
     */
    private void closeConnectionEndpoint(ProtonConnection connection) {

        // closing connection, but before closing all sink/source endpoints
        if (this.endpoints.containsKey(connection)) {
            ConnectionEndpoint endpoint = this.endpoints.get(connection);
            if (endpoint.getSource() != null) {
                endpoint.getSource().close();
            }
            if (!endpoint.getSinks().isEmpty()) {
                endpoint.getSinks().stream().forEach(sink -> sink.close());
            }
            connection.close();
            this.endpoints.remove(connection);
        }
    }

    /**
     * Handler for session closing from the remote
     *
     * @param session related Proton session closed
     */
    private void processOpenSession(ProtonSession session) {

        session.closeHandler(ar -> {

            if (ar.succeeded()) {
                ar.result().close();
            }

        }).open();
    }

    /**
     * Handler for attached link by a remote sender
     *
     * @param connection connection which the receiver link belong to
     * @param receiver receiver link created by the underlying Proton library
     *                 by which handling communication with remote sender
     */
    private void processOpenReceiver(ProtonConnection connection, ProtonReceiver receiver) {

        log.info("Remote sender attached {}", receiver.getName());

        ConnectionEndpoint endpoint = this.endpoints.get(connection);
        SourceBridgeEndpoint source = endpoint.getSource();
        // the source endpoint is only one, handling more AMQP receiver links internally
        if (source == null) {
            // TODO: the AMQP client should be able to specify the format during link attachment
            source = new AmqpSourceBridgeEndpoint<>(this.vertx, this.bridgeConfig,
                    EmbeddedFormat.JSON, new StringSerializer(), new ByteArraySerializer());

            source.closeHandler(s -> {
                endpoint.setSource(null);
            });
            source.open();
            endpoint.setSource(source);
        }
        source.handle(new AmqpEndpoint(receiver));
    }

    /**
     * Handler for attached link by a remote receiver
     *
     * @param connection connection which the sender link belong to
     * @param sender sender link created by the underlying Proton library
     *               by which handling communication with remote receiver
     */
    private void processOpenSender(ProtonConnection connection, ProtonSender sender) {

        log.info("Remote receiver attached {}", sender.getName());

        // create and add a new sink to the map
        // TODO: the AMQP client should be able to specify the format during link attachment
        SinkBridgeEndpoint<?, ?> sink = new AmqpSinkBridgeEndpoint<>(this.vertx, this.bridgeConfig,
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());

        sink.closeHandler(s -> {
            this.endpoints.get(connection).getSinks().remove(s);
        });
        sink.open();
        this.endpoints.get(connection).getSinks().add(sink);

        sink.handle(new AmqpEndpoint(sender));
    }

    /**
     * Create a new AMQP error condition
     *
     * @param error AMQP error
     * @param description description for the AMQP error condition
     * @return AMQP error condition
     */
    static ErrorCondition newError(String error, String description) {
        return new ErrorCondition(Symbol.getSymbol(error), description);
    }

    /**
     * Detach the provided link with a related error (and description)
     *
     * @param link AMQP link to detach
     * @param error AMQP error
     * @param description description for the AMQP error condition
     */
    static void detachWithError(ProtonLink<?> link, String error, String description) {
        detachWithError(link, newError(error, description));
    }

    /**
     * Detach the provided link with an AMQP error condition
     *
     * @param link AMQP link to detach
     * @param error AMQP error condition
     */
    static void detachWithError(ProtonLink<?> link, ErrorCondition error) {
        log.error("Detaching link {} due to error {}, description: {}", link, error.getCondition(), error.getDescription());
        link.setSource(null).open().setCondition(error).close();
    }

    /**
     * Instantiate and return an AMQP message converter
     *
     * @param className message converter class name to instantiate
     * @return an AMQP message converter instance
     * @throws AmqpErrorConditionException
     */
    static MessageConverter<?, ?, ?, ?> instantiateConverter(String className) throws AmqpErrorConditionException {

        if (className == null || className.isEmpty()) {
            return (MessageConverter<?, ?, ?, ?>) new AmqpDefaultMessageConverter();
        } else {
            Object instance = null;
            try {
                instance = Class.forName(className).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
                    | RuntimeException e) {
                log.debug("Could not instantiate message converter {}", className, e);
                throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_CONFIGURATION, "configured message converter class could not be instantiated: " + className);
            }

            if (instance instanceof MessageConverter) {
                return (MessageConverter<?, ?, ?, ?>) instance;
            } else {
                throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_CONFIGURATION, "configured message converter class is not an instanceof " + MessageConverter.class.getName() + ": " + className);
            }
        }
    }

    @Override
    public boolean isAlive() {
        return this.isReady;
    }

    @Override
    public boolean isReady() {
        return this.isReady;
    }
}
