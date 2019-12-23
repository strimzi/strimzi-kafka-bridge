/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Utils {

    /**
     * Retrieve the Kafka Bridge version from a text config fil, or throws an exception.
     *
     * @param releaseFile The name of the file that contains the release version
     * @return The version of the Kafka Bridge
     * @throws Exception
     */

    public static String getKafkaBridgeVersionFromFile(String releaseFile) throws Exception {

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
}
