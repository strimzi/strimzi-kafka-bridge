/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetTrackerTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetTrackerTest.class);

    private List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
    private Map<TopicPartition, OffsetAndMetadata> offsets;

    @BeforeEach
    void before() {

        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 0, null, null));
        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 1, null, null));
        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 2, null, null));
        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 3, null, null));
        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 4, null, null));
        this.records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 5, null, null));
    }

    @Test
    void fullOffsetTrackerOutOfOrder() {

        OffsetTracker offsetTracker  = new FullOffsetTracker("my_topic");

        for (ConsumerRecord<String, byte[]> record : this.records) {
            offsetTracker.track(record.partition(), record.offset(), record);
        }

        log.info("0_2 deliverd");
        offsetTracker.delivered(0, 2);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertTrue(this.offsets.isEmpty());

        log.info("0_3 deliverd");
        offsetTracker.delivered(0, 3);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertTrue(this.offsets.isEmpty());

        log.info("0_0 deliverd");
        offsetTracker.delivered(0, 0);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(0L));

        log.info("0_1 deliverd");
        offsetTracker.delivered(0, 1);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(3L));

        log.info("0_4 deliverd");
        offsetTracker.delivered(0, 4);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(4L));

        log.info("0_5 deliverd");
        offsetTracker.delivered(0, 5);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(5L));

        offsetTracker.clear();
    }

    @Test
    void fullOffsetTracker() {

        OffsetTracker offsetTracker  = new FullOffsetTracker("my_topic");

        for (ConsumerRecord<String, byte[]> record : this.records) {
            offsetTracker.track(record.partition(), record.offset(), record);
        }

        log.info("0_0 deliverd");
        offsetTracker.delivered(0, 0);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(0L));

        log.info("0_1 deliverd");
        offsetTracker.delivered(0, 1);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(1L));

        log.info("0_2 deliverd");
        offsetTracker.delivered(0, 2);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(2L));

        log.info("0_3 deliverd");
        offsetTracker.delivered(0, 3);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(3L));

        log.info("0_4 deliverd");
        offsetTracker.delivered(0, 4);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(4L));

        log.info("0_5 deliverd");
        offsetTracker.delivered(0, 5);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(5L));

        offsetTracker.clear();
    }

    @Test
    void simpleOffsetTrackerOutOfOrder() {

        OffsetTracker offsetTracker  = new SimpleOffsetTracker("my_topic");

        for (ConsumerRecord<String, byte[]> record : this.records) {
            offsetTracker.track(record.partition(), record.offset(), record);
        }

        log.info("0_2 deliverd");
        offsetTracker.delivered(0, 2);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(2L));

        log.info("0_3 deliverd");
        offsetTracker.delivered(0, 3);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(3L));

        log.info("0_0 deliverd");
        offsetTracker.delivered(0, 0);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertTrue(this.offsets.isEmpty());

        log.info("0_1 deliverd");
        offsetTracker.delivered(0, 1);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertTrue(this.offsets.isEmpty());

        log.info("0_4 deliverd");
        offsetTracker.delivered(0, 4);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(4L));

        log.info("0_5 deliverd");
        offsetTracker.delivered(0, 5);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(5L));

        offsetTracker.clear();
    }

    @Test
    void simpleOffsetTracker() {

        OffsetTracker offsetTracker  = new SimpleOffsetTracker("my_topic");

        for (ConsumerRecord<String, byte[]> record : this.records) {
            offsetTracker.track(record.partition(), record.offset(), record);
        }

        log.info("0_0 deliverd");
        offsetTracker.delivered(0, 0);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(0L));

        log.info("0_1 deliverd");
        offsetTracker.delivered(0, 1);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(1L));

        log.info("0_2 deliverd");
        offsetTracker.delivered(0, 2);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(2L));

        log.info("0_3 deliverd");
        offsetTracker.delivered(0, 3);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(3L));

        log.info("0_4 deliverd");
        offsetTracker.delivered(0, 4);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(4L));

        log.info("0_5 deliverd");
        offsetTracker.delivered(0, 5);
        this.offsets = offsetTracker.getOffsets();
        printOffsetsToCommit(this.offsets);
        offsetTracker.commit(this.offsets);
        assertThat(this.offsets.get(new TopicPartition("my_topic", 0)).offset(), is(5L));

        offsetTracker.clear();
    }

    private void printOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            log.info("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
        }
    }
}
