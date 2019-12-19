/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class StaticTest {

    public String getVersionFromFile() {

        String versionFromFile = "Unable to get version";

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("release.version"));
            versionFromFile = bufferedReader.readLine();
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return versionFromFile;
    }

    @Test
    void bridgeVersionDisplayedInStartupTest() throws IOException, InterruptedException {

        String kBVersion = getVersionFromFile();
        assertThat(kBVersion, is(not("Unable to get version")));
        Boolean versionFound = false;

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
                versionFound = true;

            // If we achieved this point, and the version was not displayed yet, test failed
            } else if ((procOutput.contains("Starting HTTP-Kafka bridge verticle")) &&  (!versionFound)) {
                assertThat("Test Failed", false);
                inputStreamReader.close();
                bridgeProc.destroy();
            }
            // Check only the first 5 lines
            lineCount++;
            if (lineCount == 5) {
                inputStreamReader.close();
                bridgeProc.destroy();
                return;
            }
        }
        bridgeProc.destroy();
        inputStreamReader.close();
    }

}
