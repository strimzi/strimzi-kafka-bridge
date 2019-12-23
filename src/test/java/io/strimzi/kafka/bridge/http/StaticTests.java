/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import static io.strimzi.kafka.bridge.utils.KafkaBridgeVersion.getKafkaBridgeVersionFromFile;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *  Static tests are those that does not applies for the regular @beforeAll, @beforeEach and @afterEach methods
 *  So this class of tests does not extends HttpBridgeTestBase
 */
public class StaticTests {

    @Test
    /**
     * Start the kafka bridge using ProcessBuilder to run the kafka_bridge_run.sh script, then check
     * if the bridge version is displayed in messages.
     */
    void bridgeVersionDisplayedInStartupTest() throws Exception {

        String kBVersion = getKafkaBridgeVersionFromFile("release.version");

        ProcessBuilder bridgeJar = new ProcessBuilder(
                "target/kafka-bridge-" + kBVersion + "/kafka-bridge-" + kBVersion + "/bin/kafka_bridge_run.sh",
                "--config-file", "target/kafka-bridge-" + kBVersion + "/kafka-bridge-" + kBVersion + "/config/application.properties");
        Process bridgeProc = bridgeJar.start();

        InputStreamReader inputStreamReader = new InputStreamReader(bridgeProc.getInputStream());
        BufferedReader bufferedInputReader = new BufferedReader(inputStreamReader);

        int lineCount = 0;
        String procOutput;
        while ((procOutput = bufferedInputReader.readLine()) != null) {

            if ((procOutput.contains("Strimzi Kafka Bridge")) && (procOutput.contains("starting"))) {
                assertThat(procOutput, containsString("Strimzi Kafka Bridge " + kBVersion + " is starting"));
                break;
            } else {
                // Check only the first 5 lines
                lineCount++;
                if (lineCount == 5) {
                    inputStreamReader.close();
                    bridgeProc.destroy();
                    assertThat("Test Failed", false);
                }
            }
        }
        bridgeProc.destroy();
        inputStreamReader.close();
    }
}
