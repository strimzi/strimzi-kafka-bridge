/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.rateLimiting;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.grid.GridBucketState;
import io.github.bucket4j.grid.ProxyManager;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class RlDeploymentOperations {
    
    private static final String BUCKET_MAP = "bucket-map";
    
    
    /*
     * returns the namespace in which the bridge pods are in. The value is retrieved from the filesystem of the pods
     */
    private static String getPodNamespace() {
        String namespace;
        try {            
            namespace = new String(Files.readAllBytes(Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/namespace")));
        } catch (Exception e) {
            namespace = "";
            e.printStackTrace();
        }
        return namespace;
        
    }
    
    
    /*
     * generate the configuration of the hazelcast cluster
     */
    private static Config getHazelcastConfig(int evictionTime) {
        Config hazelcastConfig = new Config();
        hazelcastConfig.getNetworkConfig().setPort(5701);
        
        hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        
        hazelcastConfig.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(true)
            .setProperty("namespace", getPodNamespace())
            .setProperty("resolve-not-ready-addresses", "true")
            .setProperty("service-name", "bridge-hazelcast-service")
            .setProperty("service-port", "5701");
        
        LoggerFactory.getLogger(RlDeploymentOperations.class).info("Eviction time for Hazelcast (GLOBAL RL): " + evictionTime);
        hazelcastConfig.getMapConfig(BUCKET_MAP)
            .setTimeToLiveSeconds(evictionTime)
            .setEvictionPolicy(EvictionPolicy.LRU);
        
        return hazelcastConfig;
    }
    
    public static ProxyManager<String> startHazelcast(Handler<AsyncResult<Void>> resultHandler, int evictionTime) {
        try {
            Config hazelcastConfig = getHazelcastConfig(evictionTime);
            
            HazelcastInstance hazelcastCluster = Hazelcast.newHazelcastInstance(hazelcastConfig);
            IMap<String, GridBucketState> map = hazelcastCluster.getMap(BUCKET_MAP);
            ProxyManager<String> globalBuckets = Bucket4j.extension(io.github.bucket4j.grid.hazelcast.Hazelcast.class).proxyManagerForMap(map);
            resultHandler.handle(Future.succeededFuture());            
            return globalBuckets;
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture("Cannot deploy Hazelcast cluster"));
        }
        return null;
    }
    
    public static Map<String, BucketDate> startLocalRlMap(BridgeConfig bridgeConfig) {
        
        Map<String, BucketDate> localBuckets = null;
        if (bridgeConfig.getHttpConfig().isRateLimitingEnabled() && bridgeConfig.getRateLimitingPolicy().usesLocalRL()) {
            // initialize a bucket for local use only
            localBuckets = new ConcurrentHashMap<>();
            new CleanLocalMapThread(localBuckets, bridgeConfig.getRateLimitingPolicy().getWindowTimeLocalRl());
        }
        return localBuckets;
    }
    
    
}
