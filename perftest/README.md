# Performance tests

This folder contains a [JMeter](https://jmeter.apache.org/) JMX configuration file describing a test plan with the following operations:

* consumers creation, topics subscription, polling for getting records in a loop, and final consumers deletion
* producers sending records to topics in a loop

The test plan is configurable. You can change the number of consumers/producers (JMeter threads) and the number of loop cycles for sending/receiving records.

It needs a set of plugins in order to show some graphs.
For this reason, you need to download the JMeter Plugins Manager from the [jmeter-plugins.org](https://jmeter-plugins.org/get/) website and put it into the `lib/ext` folder.
When opening the JMX configuration file for the first time, JMeter will ask to install the plugins to run the test plan.