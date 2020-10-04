/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.rateLimiting;

import java.time.LocalDateTime;
import java.util.Optional;

import io.github.bucket4j.Bucket;

public class BucketDate {
    
    Optional<Bucket> bucket;
    LocalDateTime lastAccessTime;
    
    public BucketDate(Optional<Bucket> bucket) {
        this.bucket = bucket;
        this.lastAccessTime = null;
    }
    
    public LocalDateTime getLastAccessTime() {
        return this.lastAccessTime;
    }
    
    public void setLocalDate() {
        this.lastAccessTime = LocalDateTime.now();
    }
    
    public Optional<Bucket> getOptionalBucket() {
        return this.bucket;
    }
}
