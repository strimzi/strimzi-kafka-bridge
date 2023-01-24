/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.ConfigRetriever;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@QuarkusMain
public class Main {

    public static void main(String... args) {
        System.out.println("Running main method with " + args.length + " args = " +  args);

        try {
            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);
            String absoluteFilePath = absoluteFilePath(commandLine.getOptionValue("config-file"));
            System.out.println("absoluteFilePath = " + absoluteFilePath);

            Map<String, Object> config = ConfigRetriever.getConfig(absoluteFilePath(commandLine.getOptionValue("config-file")));
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
            System.out.println("bridgeConfig = " + bridgeConfig);

        } catch (ParseException | IOException e) {
            System.exit(1);
        }

        Quarkus.run(args);
    }

    /**
     * Generate the command line options
     *
     * @return command line options
     */
    private static Options generateOptions() {

        Option configFileOption = Option.builder()
                .required(true)
                .hasArg(true)
                .longOpt("config-file")
                .desc("Configuration file with bridge parameters")
                .build();

        Options options = new Options();
        options.addOption(configFileOption);
        return options;
    }

    private static String absoluteFilePath(String arg) {
        // return the file path as absolute (if it's relative)
        return arg.startsWith(File.separator) ? arg : System.getProperty("user.dir") + File.separator + arg;
    }
}
