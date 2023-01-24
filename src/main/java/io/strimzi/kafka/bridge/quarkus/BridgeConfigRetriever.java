/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.CommandLineArguments;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.ConfigRetriever;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Retriever of the bridge configuration on startup from the provided configuration file
 */
@ApplicationScoped
public class BridgeConfigRetriever {

    @Inject
    Logger log;

    @Inject
    @CommandLineArguments
    String[] args;

    private BridgeConfig bridgeConfig;

    public void onStart(@Observes StartupEvent ev) {
        try {
            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);
            Map<String, Object> config = ConfigRetriever.getConfig(absoluteFilePath(commandLine.getOptionValue("config-file")));
            bridgeConfig = BridgeConfig.fromMap(config);
            log.infof("Bridge configuration %s", bridgeConfig);
        } catch (ParseException | IOException e) {
            log.error("Error starting the bridge", e);
            Quarkus.asyncExit();
        }
    }

    /**
     * @return the bridge configuration
     */
    public BridgeConfig config() {
        return bridgeConfig;
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
