# Performance tests

This folder contains a [JMeter](https://jmeter.apache.org/) configuration file decribing a test plan with:

* consumers creation, topics subscription, polling for getting records in a loop, and final consumers deletion
* producers sending records to topics in a loop

The test plan is configurable changing the number of consumers/producers (JMeter threads) and the number of loop cycles for sending/receiving records.