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

    public String getVersionFromFile(String releaseFile) throws Exception {

        String versionFromFile;

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(releaseFile));
            versionFromFile = bufferedReader.readLine();
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            throw new Exception("File not found : " + releaseFile);
        } catch (IOException e) {
            throw new Exception("Unable to open file : " + releaseFile);
        }

        if ((versionFromFile == null) || (versionFromFile.isEmpty())) {
            throw new Exception("Unable to get Version from file : " + releaseFile);
        }
        return versionFromFile;
    }

    @Test
    void bridgeVersionDisplayedInStartupTest() throws Exception {

        String kBVersion = getVersionFromFile("release.version");
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
