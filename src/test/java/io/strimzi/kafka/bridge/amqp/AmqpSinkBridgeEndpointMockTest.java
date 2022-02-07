/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.SinkTopicSubscription;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AmqpSinkBridgeEndpointMockTest {

    private static Map<String, Object> config = new HashMap<>();

    static {
        config.put(AmqpConfig.AMQP_ENABLED, true);
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    class MockRecordProducer {

        private String topic;
        private int partition;
        private long offset;

        MockRecordProducer(String topic, int partition, long initialOffset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = initialOffset;
        }

        protected <K, V> KafkaConsumerRecord<K, V> mockRecord(Supplier<K> key, Supplier<V> value) {
            KafkaConsumerRecord<K, V> mockVertxRecord = mock(KafkaConsumerRecord.class);
            when(mockVertxRecord.topic()).thenReturn(this.topic);
            when(mockVertxRecord.partition()).thenReturn(this.partition);
            when(mockVertxRecord.offset()).thenReturn(this.offset);

            mockVertxRecord = new KafkaConsumerRecordImpl<>(new ConsumerRecord<>(this.topic, this.partition, this.offset, key != null ? key.get() : null, value != null ? value.get() : null));

            this.offset++;
            return mockVertxRecord;
        }
    }


    protected <V, K> KafkaConsumer<K, V> installConsumerSpy(AmqpSinkBridgeEndpoint<K, V> endpoint)
            throws NoSuchFieldException, IllegalAccessException {
        Field consumerField = AmqpSinkBridgeEndpoint.class.getSuperclass().getDeclaredField("consumer");
        consumerField.setAccessible(true);
        KafkaConsumer<K, V> consumer = (KafkaConsumer<K, V>) consumerField.get(endpoint);
        KafkaConsumer<K, V> consumerSpy = spy(consumer);
        consumerField.set(endpoint, consumerSpy);
        return consumerSpy;
    }


    protected ProtonSender mockSender(ProtonQoS qos, String remoteAddress) {

        ProtonSender mockSender = mock(ProtonSender.class);
        Source remoteSource = new Source();
        remoteSource.setAddress(remoteAddress);
        when(mockSender.getRemoteSource()).thenReturn(remoteSource);

        Source localSource = new Source();
        localSource.setAddress("my_topic/group.id/my_group");
        when(mockSender.getSource()).thenReturn(localSource);
        when(mockSender.getQoS()).thenReturn(qos);
        when(mockSender.setSource(any())).thenReturn(mockSender);
        when(mockSender.open()).thenReturn(mockSender);
        when(mockSender.setCondition(any())).thenReturn(mockSender);
        when(mockSender.closeHandler(any())).thenReturn(mockSender);
        when(mockSender.detachHandler(any())).thenReturn(mockSender);

        return mockSender;
    }


    protected KafkaConsumerRecords<String, byte[]> mockRecords() {
        KafkaConsumerRecords<String, byte[]> mockRecords = mock(KafkaConsumerRecords.class);
        when(mockRecords.size()).thenReturn(1);
        // recordAt should not be called by the endpoint
        // but just to be sure...
        when(mockRecords.recordAt(anyInt())).thenThrow(UncheckedIOException.class);
        return mockRecords;
    }

    /**
     * Assert that the sender is closed with the given error condition and message;
     * @param mockSender
     * @param errorCondition
     * @param errorMessage
     */
    protected void assertDetach(ProtonSender mockSender,
            String errorCondition,
            String errorMessage) {
        ArgumentCaptor<ErrorCondition> errorCap = ArgumentCaptor.forClass(ErrorCondition.class);
        verify(mockSender).setCondition(errorCap.capture());
        verify(mockSender).close();
        assertThat(errorCap.getValue().getCondition().toString(), is(errorCondition));
        assertThat(errorCap.getValue().getDescription(), is(errorMessage));
    }

    // Test normal flow in AT_MOST_ONCE mode.
    @Test
    @Disabled
    public <K, V> void normalFlow_AtMostOnce() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        MockRecordProducer recordProducer = new MockRecordProducer(topic, 0, 0L);
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();

        // Create a mock for the sender
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/my_group");

        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        // Now the consumer is set we can add a spy for it
        // ( so we can inspect KafkaConsumer.commit() )
        KafkaConsumer<K, V> consumerSpy = installConsumerSpy(endpoint);

        // Simulate vertx-kafka-client delivering a record
        Method handler = endpoint.getClass().getSuperclass().getDeclaredMethod("handleKafkaRecord", KafkaConsumerRecord.class);
        handler.setAccessible(true);
        handler.invoke(endpoint, recordProducer.mockRecord(null, () -> "Hello, world".getBytes()));

        ArgumentCaptor<Handler<AsyncResult<Void>>> handlerCap = ArgumentCaptor.forClass(Handler.class);

        verify(consumerSpy).commit(handlerCap.capture());
        handlerCap.getValue().handle(new AsyncResult<Void>() {

            @Override
            public Void result() {
                return null;
            }

            @Override
            public Throwable cause() {
                return null;
            }

            @Override
            public boolean succeeded() {
                return true;
            }

            @Override
            public boolean failed() {
                return false;
            }
        });

        // verify sender.send() was called and grab the arguments
        ArgumentCaptor<byte[]> tagCap = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<Message> messageCap = ArgumentCaptor.forClass(Message.class);
        verify(mockSender).send(tagCap.capture(), messageCap.capture());
        Message message = messageCap.getValue();
        // Assert the transformed message was as expected
        assertThat(topic + "/group.id/my_group", is(message.getAddress()));

        assertThat(((Data) message.getBody()).getValue().getArray(), is("Hello, world".getBytes()));
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)), is(topic));
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION)), is(0));
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION)), is(0L));

        // TODO test closure (commit)
    }

    // Test normal flow in AT_LEAST_ONCE mode.
    @Test
    public <K, V> void normalFlow_AtLeastOnce() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        MockRecordProducer recordProducer = new MockRecordProducer(topic, 0, 0L);
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();

        // Create a mock for the sender
        ProtonSender mockSender = mockSender(ProtonQoS.AT_LEAST_ONCE, topic + "/group.id/my_group");

        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        // Now the consumer is set we can add a spy for it
        // ( so we can inspect KafkaConsumer.commit() )
        KafkaConsumer<K, V> consumerSpy = installConsumerSpy(endpoint);

        // Simulate vertx-kafka-client delivering a batch
        Method batchHandler = endpoint.getClass().getSuperclass().getDeclaredMethod("handleKafkaBatch", KafkaConsumerRecords.class);
        batchHandler.setAccessible(true);
        KafkaConsumerRecords<String, byte[]> mockRecords = mockRecords();

        // Simulate vertx-kafka-client delivering a record
        Method handler = endpoint.getClass().getSuperclass().getDeclaredMethod("handleKafkaRecord", KafkaConsumerRecord.class);
        handler.setAccessible(true);

        // Kafka batch of 1
        batchHandler.invoke(endpoint, mockRecords);
        handler.invoke(endpoint, recordProducer.mockRecord(null, () -> "Hello, world".getBytes()));

        // verify sender.send() was called and grab the arguments
        ArgumentCaptor<byte[]> tagCap = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<Message> messageCap = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<Handler<ProtonDelivery>> handlerCap = ArgumentCaptor.forClass(Handler.class);
        verify(mockSender).send(tagCap.capture(), messageCap.capture(), handlerCap.capture());
        Message message = messageCap.getValue();

        // Assert the transformed message was as expected
        assertThat(message.getAddress(), is(topic + "/group.id/my_group"));
        assertThat(((Data) message.getBody()).getValue().getArray(), is("Hello, world".getBytes()));
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)), is(topic));
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION)), is(0));
        assertThat(messageAnnotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION)), is(0L));

        // Simulate Proton delivering settlement
        ProtonDelivery mockDelivery = mock(ProtonDelivery.class);
        when(mockDelivery.getTag()).thenReturn(tagCap.getValue());
        handlerCap.getValue().handle(mockDelivery);

        // We now have to deliver another batch
        // because the AMQP delivery callback for the first message
        // fires after commitOffsets() is called for the last message of the first batch

        // Kafka batch of 1
        batchHandler.invoke(endpoint, mockRecords);
        handler.invoke(endpoint, recordProducer.mockRecord(null, () -> "Hello, world".getBytes()));

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitMapCap = ArgumentCaptor.forClass(Map.class);
        verify(consumerSpy).commit(commitMapCap.capture(), any(Handler.class));

        // TODO test closure (commit)
    }

    // When happens when the address is malformed?
    @Test
    public <K, V> void address_badAddressNoGroupId() throws Exception {
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "missing group id delimiter");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_NO_GROUPID,
                "Mandatory group.id not specified in the address");
    }

    // When happens when the topic is empty?
    @Test
    public <K, V> void address_badAddressEmptyTopic() throws Exception {
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "/group.id/blah");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_NO_GROUPID,
                "Empty topic in specified address");
    }

    // When happens when the consumer group is empty?
    @Test
    public <K, V> void address_badAddressEmptyGroup() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_NO_GROUPID,
                "Empty consumer group in specified address");
    }

    // When happens when partition filter is not an Integer?
    @Test
    public <K, V> void filters_nonIntegerPartitionFilter() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/blah");
        // Call handle()
        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER), "not an integer");
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER), 10L);
        ((Source) mockSender.getRemoteSource()).setFilter(filter);
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_WRONG_PARTITION_FILTER,
                "Wrong partition filter");
    }

    // When happens when offset filter is not a Long?
    @Test
    public <K, V> void filters_nonLongOffsetFilter() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/blah");
        // Call handle()
        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER), 0);
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER), "not a long");
        ((Source) mockSender.getRemoteSource()).setFilter(filter);
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                // TODO really?
                AmqpBridge.AMQP_ERROR_WRONG_OFFSET_FILTER,
                "Wrong offset filter");
    }

    // When happens when the partition filter &lt; 0?
    @Test
    public <K, V> void filters_negativeIntegerPartitionFilter() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/blah");
        // Call handle()
        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER), -1);
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER), 10L);
        ((Source) mockSender.getRemoteSource()).setFilter(filter);
        endpoint.handle(new AmqpEndpoint(mockSender));

        ArgumentCaptor<ErrorCondition> errorCap = ArgumentCaptor.forClass(ErrorCondition.class);
        verify(mockSender).setCondition(errorCap.capture());
        verify(mockSender).close();

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_WRONG_FILTER,
                "Wrong filter");
    }

    // When happens when the offset filter is &lt; 0?
    @Test
    public <K, V> void filters_negativeLongOffsetFilter() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/blah");
        // Call handle()
        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER), 0);
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER), -10L);
        ((Source) mockSender.getRemoteSource()).setFilter(filter);
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_WRONG_FILTER,
                "Wrong filter");
    }

    // When happens when there's a filter for offset, but not for partition?
    @Test
    public <K, V> void filters_offsetFilterButNoPartitionFilter() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/blah");
        // Call handle()
        Map<Symbol, Object> filter = new HashMap<>();
        //filter.put(Symbol.getSymbol(Bridge.AMQP_PARTITION_FILTER), 0);
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER), 10L);
        ((Source) mockSender.getRemoteSource()).setFilter(filter);
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_NO_PARTITION_FILTER,
                "No partition filter specified");
    }

    @Test
    public <K, V> void config_NoSuchConverterClass() throws AmqpErrorConditionException {
        Vertx vertx = Vertx.vertx();
        BridgeConfig config = BridgeConfig.fromMap(AmqpSinkBridgeEndpointMockTest.config);
        config.getAmqpConfig().setMessageConverter("foo.bar.Baz");
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, config,
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());

        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_CONFIGURATION,
                "configured message converter class could not be instantiated: foo.bar.Baz");
    }

    @Test
    public <K, V> void config_ConverterWrongType() throws AmqpErrorConditionException {
        Vertx vertx = Vertx.vertx();
        BridgeConfig config = BridgeConfig.fromMap(AmqpSinkBridgeEndpointMockTest.config);
        config.getAmqpConfig().setMessageConverter("java.util.HashSet");
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, config,
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_CONFIGURATION,
                "configured message converter class is not an instanceof io.strimzi.kafka.bridge.converter.MessageConverter: java.util.HashSet");
    }

    static class NoNullaryCtor<K, V, M, C> implements MessageConverter<K, V, M, C> {
        private NoNullaryCtor() {
            throw new RuntimeException();
        }

        @Override
        public KafkaProducerRecord<K, V> toKafkaRecord(String kafkaTopic, Integer partition, M message) {
            return null;
        }

        @Override
        public List<KafkaProducerRecord<K, V>> toKafkaRecords(String kafkaTopic, Integer partition, C messages) {
            return null;
        }

        @Override
        public M toMessage(String address, KafkaConsumerRecord<K, V> record) {
            return null;
        }

        @Override
        public C toMessages(KafkaConsumerRecords<K, V> records) {
            return null;
        }
    }

    @Test
    public <K, V> void config_ConverterNoDefaultConstructor() throws AmqpErrorConditionException {
        Vertx vertx = Vertx.vertx();
        BridgeConfig config = BridgeConfig.fromMap(AmqpSinkBridgeEndpointMockTest.config);
        config.getAmqpConfig().setMessageConverter(NoNullaryCtor.class.getName());
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, config,
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_CONFIGURATION,
                "configured message converter class could not be instantiated: io.strimzi.kafka.bridge.amqp.AmqpSinkBridgeEndpointMockTest$NoNullaryCtor");
    }

    static class CtorThrows<K, V, M, C> implements MessageConverter<K, V, M, C> {
        public CtorThrows() {
            throw new RuntimeException();
        }

        @Override
        public KafkaProducerRecord<K, V> toKafkaRecord(String kafkaTopic, Integer partition, M message) {
            return null;
        }

        @Override
        public List<KafkaProducerRecord<K, V>> toKafkaRecords(String kafkaTopic, Integer partition, C messages) {
            return null;
        }

        @Override
        public M toMessage(String address, KafkaConsumerRecord<K, V> record) {
            return null;
        }

        @Override
        public C toMessages(KafkaConsumerRecords<K, V> records) {
            return null;
        }

    }

    @Test
    public <K, V> void config_ConverterDefaultConstructorThrows() throws AmqpErrorConditionException {
        Vertx vertx = Vertx.vertx();
        BridgeConfig config = BridgeConfig.fromMap(AmqpSinkBridgeEndpointMockTest.config);
        config.getAmqpConfig().setMessageConverter(CtorThrows.class.getName());
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, config,
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, "");
        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));

        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_CONFIGURATION,
                "configured message converter class could not be instantiated: io.strimzi.kafka.bridge.amqp.AmqpSinkBridgeEndpointMockTest$CtorThrows");
    }
    // What happens if the requested kafka topic doesn't exist?
    @Test
    public <K, V> void noSuchTopic() {

    }
    // What happens if we can't get the partitions for the given topic?
    // @throws AmqpErrorConditionException
    @Test
    @Disabled
    public <K, V> void partitionsForFails() throws Exception {
        String topic = "my_topic";
        Vertx vertx = Vertx.vertx();
        AmqpSinkBridgeEndpoint<K, V> endpoint = (AmqpSinkBridgeEndpoint) new AmqpSinkBridgeEndpoint<>(vertx, BridgeConfig.fromMap(config),
                EmbeddedFormat.JSON, new StringDeserializer(), new ByteArrayDeserializer());
        endpoint.open();

        // Create a mock for the sender
        ProtonSender mockSender = mockSender(ProtonQoS.AT_MOST_ONCE, topic + "/group.id/my_group");

        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER), 0);
        ((Source) mockSender.getRemoteSource()).setFilter(filter);

        // Call handle()
        endpoint.handle(new AmqpEndpoint(mockSender));
        Method handler = endpoint.getClass().getSuperclass().getDeclaredMethod("partitionsForHandler", AsyncResult.class);
        handler.setAccessible(true);
        handler.invoke(endpoint, new AsyncResult<List<PartitionInfo>>() {

            Throwable cause = new Exception();

            @Override
            public List<PartitionInfo> result() {
                Assertions.fail();
                return null;
            }

            @Override
            public Throwable cause() {
                return this.cause;
            }

            @Override
            public boolean succeeded() {
                return false;
            }

            @Override
            public boolean failed() {
                return true;
            }
        });

        SinkTopicSubscription topicSubscription = new SinkTopicSubscription(topic, 0, null);
        assertDetach(mockSender,
                AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
                "Error getting partition info for topic " + Collections.singleton(topicSubscription));
    }

    // TODO kafka partition doesn't exist
    // TODO assign fails
    // TODO seek fails
    // TODO partition assign & revoke
    // TODO kafka commit fails
    // TODO proton delivery not accepted
    // TODO converter throws (each direction)
    // TODO converter returns null (each direction)
    // TODO flow control

}
