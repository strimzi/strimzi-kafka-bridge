/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package enmasse.kafka.bridge;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.ext.unit.TestContext;

/**
 * Base class for tests providing a Kafka cluster
 */
public class KafkaClusterTestBase {

  private static File dataDir;
  protected static KafkaCluster kafkaCluster;

  protected static KafkaCluster kafkaCluster() {
    if (kafkaCluster != null) {
      throw new IllegalStateException();
    }
    dataDir = Testing.Files.createTestingDirectory("cluster");
    Properties kafkaConfig = new Properties();
    // TODO : to remove when moving to new Debezium 0.7.0 (https://github.com/debezium/debezium/pull/358)
    kafkaConfig.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), String.valueOf(Long.MAX_VALUE));
    kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9092).withKafkaConfiguration(kafkaConfig);
    return kafkaCluster;
  }

  @BeforeClass
  public static void setUp(TestContext ctx) throws IOException {
    kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true).addBrokers(1).startup();
  }


  @AfterClass
  public static void tearDown(TestContext ctx) {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
      kafkaCluster = null;
      boolean delete = dataDir.delete();
      // If files are still locked and a test fails: delete on exit to allow subsequent test execution
      if(!delete) {
        dataDir.deleteOnExit();
      }
    }
  }
}