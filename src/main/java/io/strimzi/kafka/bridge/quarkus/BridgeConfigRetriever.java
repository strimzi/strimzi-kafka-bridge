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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

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

    @ConfigProperty(name = "quarkus.config.locations")
    Optional<String> quarkusConfigFile;

    public void onStart(@Observes StartupEvent ev) {
        try {
            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);
            String configFile = commandLine.hasOption("config-file") ? commandLine.getOptionValue("config-file") : quarkusConfigFile.orElse(null);
            if (configFile != null) {
                Map<String, Object> config = ConfigRetriever.getConfig(absoluteFilePath(configFile));
                bridgeConfig = BridgeConfig.fromMap(config);
                log.infof("Bridge configuration %s", bridgeConfig);
            } else {
                log.error("Error starting the bridge: no '--config-file' option or 'quarkus.config.locations' system property set");
                Quarkus.asyncExit();
            }
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
                .required(false)
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
