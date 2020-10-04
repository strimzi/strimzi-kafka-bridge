/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.rateLimiting;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanLocalMapThread implements Runnable {

    private Map<String, BucketDate> locaBuckets;
    private int evictionTime;
    
    private static final Logger log = LoggerFactory.getLogger(CleanLocalMapThread.class);

    
    public CleanLocalMapThread(Map<String, BucketDate> localBuckets, int evictionTime) {
        this.locaBuckets = localBuckets;
        this.evictionTime = evictionTime;

        LoggerFactory.getLogger(CleanLocalMapThread.class).info("Eviction time for Map (LOCAL RL): " + evictionTime);
        Thread thread = new Thread(this, "LocalBucketMapCleanerDaemon");
        thread.start();
        
    }
    
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(Duration.ofSeconds(40).toMillis());
                synchronized (locaBuckets) {
                    log.info("Cleaning local buckets map...");
                    for (Map.Entry<String, BucketDate> bucketDate : locaBuckets.entrySet()) {
                        
                        Duration difference = Duration.between(bucketDate.getValue().getLastAccessTime(), LocalDateTime.now());
                        if (difference.getSeconds() > evictionTime) {
                            locaBuckets.remove(bucketDate.getKey());
                        }
                    }
                    log.info("Local map cleaned");
                    locaBuckets.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
