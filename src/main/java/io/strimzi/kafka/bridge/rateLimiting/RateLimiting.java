/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.rateLimiting;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.HttpSourceBridgeEndpoint;
import io.strimzi.kafka.bridge.http.HttpUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConfigurationBuilder;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.Refill;
import io.github.bucket4j.local.LocalBucket;
import io.github.bucket4j.local.LocalBucketBuilder;

@SuppressWarnings("checkstyle:ParameterNumber")
public class RateLimiting {

    private static final int TOPIC_GLOBAL = 0;
    private static final int TOPIC_LOCAL = 1;
    private static final int DEFAULT_GLOABAL = 2;
    private static final int DEFAULT_LOCAL = 3;

    private static final String IGNORE_IP = "_ignore_ip_";
    private static final String IGNORE_HEADER = "_ignore_header_";

    public static void allowRequest(HttpSourceBridgeEndpoint<?, ?> bridge, RoutingContext routingContext, String topic) {

        Optional<RateLimitSingleLimit> singleLimit;
        String ipAddress = routingContext.request().remoteAddress().host();
        Map<String, String> headers = bridge.getHeaders();

        long numOfTokens = getNumberOfTokens(routingContext, bridge, topic);
        if (numOfTokens == -1) {
            return;
        }

        RateLimitingPolicy rateLimitingPolicy = bridge.getRateLimitingPolicy();

        switch (getRateLimitingStrategy(rateLimitingPolicy, topic)) {
            case TOPIC_GLOBAL:
                singleLimit = getSingleLimit(rateLimitingPolicy.getLimits().get(topic), headers);
                allowRequestGlobalStrategy(routingContext, topic, ipAddress, bridge, numOfTokens, singleLimit, true, headers);
                break;
            case TOPIC_LOCAL:
                singleLimit = getSingleLimit(rateLimitingPolicy.getLimits().get(topic), headers);
                allowRequestLocalStrategy(routingContext, topic, ipAddress, bridge, numOfTokens, singleLimit, true, headers);
                break;
            case DEFAULT_GLOABAL:
                singleLimit = getSingleLimit(rateLimitingPolicy.getDefaultLimitInstance(), bridge.getHeaders());
                allowRequestGlobalStrategy(routingContext, topic, ipAddress, bridge, numOfTokens, singleLimit, false, headers);
                break;
            case DEFAULT_LOCAL:
                singleLimit = getSingleLimit(rateLimitingPolicy.getDefaultLimitInstance(), bridge.getHeaders());
                allowRequestLocalStrategy(routingContext, topic, ipAddress, bridge, numOfTokens, singleLimit, false, headers);
                break;
            default:
                bridge.sendMessagesToTopic(routingContext, topic);
                break;
        }
    }

    private static void allowRequestGlobalStrategy(RoutingContext routingContext, String topic, String ipAddress, 
            HttpSourceBridgeEndpoint<?, ?> bridge, long numOfTokens, Optional<RateLimitSingleLimit> limit, 
            boolean includeTopicInBucketName, Map<String, String> headers) {

        if (limit.isPresent()) {
            // the request should be rate limited, we can give for granted that some limit (sec/min) exists
            String bucketName = getBucketName(limit.get(), topic, headers, ipAddress, includeTopicInBucketName);

            Supplier<BucketConfiguration> supplier = getSupplier(limit.get(), headers);
            Bucket requestBucket = bridge.getGlobalBuckets().getProxy(bucketName, supplier);
            Bandwidth[] bandwidths = supplier.get().getBandwidths();
            long bucketCapacity = getBucketCapacity(bandwidths, routingContext);
            if (bucketCapacity < numOfTokens) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.FORBIDDEN.code(), 
                        BridgeContentType.JSON, getTooManyTokensErrorMessage(topic, bucketCapacity));
            } else {
                CompletableFuture.supplyAsync(() -> requestBucket.asAsync().tryConsumeAndReturnRemaining(numOfTokens))
                    .thenAccept(result -> {
                        try {
                            processRateLimitingResults(result.get(), routingContext, topic, bridge);
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), 
                                    BridgeContentType.JSON, new JsonObject().toBuffer());
                        }
                    });        
            }            
        } else {
            bridge.sendMessagesToTopic(routingContext, topic);
        }
    }
    
    private static void allowRequestLocalStrategy(RoutingContext routingContext, String topic, String ipAddress, 
            HttpSourceBridgeEndpoint<?, ?> bridge, long numOfTokens, Optional<RateLimitSingleLimit> limit, 
            boolean includeTopicInBucketName, Map<String, String> headers) {
        if (limit.isPresent()) {
            String bucketName = getBucketName(limit.get(), topic, headers, ipAddress, includeTopicInBucketName);
            BucketDate bucketDate = bridge.getLocalBuckets().computeIfAbsent(bucketName, key -> buildNewBucket(limit.get(), headers));
            bucketDate.setLocalDate();
            LocalBucket requestBucket = (LocalBucket) bucketDate.getOptionalBucket().get();
            Bandwidth[] bandwidths = requestBucket.getConfiguration().getBandwidths();
            long bucketCapacity = getBucketCapacity(bandwidths, routingContext);
            if (bucketCapacity < numOfTokens) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.FORBIDDEN.code(), BridgeContentType.JSON, 
                        getTooManyTokensErrorMessage(topic, bucketCapacity));
            } else {
                ConsumptionProbe probe = requestBucket.tryConsumeAndReturnRemaining(numOfTokens);
                processRateLimitingResults(probe, routingContext, topic, bridge);
            }
        } else {
            bridge.sendMessagesToTopic(routingContext, topic);
        }
    }

    public static void processRateLimitingResults(ConsumptionProbe probe, RoutingContext routingContext, String topic, 
            HttpSourceBridgeEndpoint<?, ?> bridge) {
        long nanosToWait = probe.getNanosToWaitForRefill();
        if (nanosToWait > 0) {
            routingContext.response().putHeader("X-Millis-Until-Refill", Integer.toString((int) Math.ceil(nanosToWait / 1_000_000)));
        }
        if (probe.isConsumed()) {
            routingContext.response().putHeader("X-Limit-Remaining", Long.toString(probe.getRemainingTokens()));
            bridge.sendMessagesToTopic(routingContext, topic);
        } else {
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 
                    BridgeContentType.JSON, getTooManyRequestsErrorMessage(topic, probe.getNanosToWaitForRefill()));
        }
    }

    private static BucketDate buildNewBucket(RateLimitSingleLimit singleLimit, Map<String, String> headers) {
        LocalBucketBuilder builder = Bucket4j.builder();
        addLimit(singleLimit.getLimitSeconds(), singleLimit.getLimitMinutes(), singleLimit.getLimitHours(), builder);
        return new BucketDate(Optional.of(builder.build()));
    }

    private static Supplier<BucketConfiguration> getSupplier(RateLimitSingleLimit singleLimit, Map<String, String> headers) {
        ConfigurationBuilder configurationBuilder = Bucket4j.configurationBuilder();
        addLimit(singleLimit.getLimitSeconds(), singleLimit.getLimitMinutes(), singleLimit.getLimitHours(), configurationBuilder);
        return () -> configurationBuilder.build();                
    }

    /**
     * 
     * @return the RateLimitSingleLimit that applies to the request or an empty Optional if no limit applies to it
     */
    private static Optional<RateLimitSingleLimit> getSingleLimit(TopicLimit limit, Map<String, String> headers) {
        for (RateLimitSingleLimit currentLimit : limit.getLimits()) {
            String header = currentLimit.getHeaderName();
            String aggregationHeader = currentLimit.getHeaderAggregation();
            if (header.length() == 0) {
                if ((aggregationHeader.length() > 0 && headers.containsKey(aggregationHeader)) || aggregationHeader.length() == 0) {
                    return Optional.of(currentLimit);
                }
            } else if (header.length() > 0 && headers.containsKey(header)) {
                String pattern = currentLimit.getHeaderPattern();
                Matcher matcher = Pattern.compile(pattern).matcher(headers.get(header));
                if (matcher.find()) {
                    if ((aggregationHeader.length() > 0 && headers.containsKey(aggregationHeader)) || aggregationHeader.length() == 0) {
                        return Optional.of(currentLimit);
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static void addLimit(int limitSeconds, int limitMinutes, int limitHours, ConfigurationBuilder configurationBuilder) {
        if (limitSeconds > 0) {
            configurationBuilder.addLimit(Bandwidth.classic(limitSeconds, 
                    Refill.intervally(limitSeconds, Duration.ofSeconds(1))).withInitialTokens(limitSeconds));
        }
        if (limitMinutes > 0) {
            configurationBuilder.addLimit(Bandwidth.classic(limitMinutes, 
                    Refill.intervally(limitMinutes, Duration.ofMinutes(1))).withInitialTokens(limitMinutes));
        }
        if (limitHours > 0) {
            configurationBuilder.addLimit(Bandwidth.classic(limitHours, 
                    Refill.intervally(limitHours, Duration.ofHours(1))).withInitialTokens(limitHours));
        }
    }

    private static void addLimit(int limitSeconds, int limitMinutes, int limitHours, LocalBucketBuilder localBucketBuilder) {
        if (limitSeconds > 0) {
            localBucketBuilder.addLimit(Bandwidth.classic(limitSeconds, 
                    Refill.intervally(limitSeconds, Duration.ofSeconds(1))).withInitialTokens(limitSeconds));
        }
        if (limitMinutes > 0) {
            localBucketBuilder.addLimit(Bandwidth.classic(limitMinutes, 
                    Refill.intervally(limitMinutes, Duration.ofMinutes(1))).withInitialTokens(limitMinutes));
        }
        if (limitHours > 0) {
            localBucketBuilder.addLimit(Bandwidth.classic(limitHours, 
                    Refill.intervally(limitHours, Duration.ofHours(1))).withInitialTokens(limitHours));
        }
        
    }

    private static Buffer getTooManyRequestsErrorMessage(String topic, long nanosToWait) {
        return new JsonObject().put("error", "too many requests")
                .put("X-Topic", topic)
                .put("X-Millis-Until-Refill", (int) Math.ceil(nanosToWait / 1_000_000))
                .toBuffer();
    }

    private static Buffer getTooManyTokensErrorMessage(String topic, long maximum) {
        return new JsonObject()
                .put("X-Error", "The number of messages you tried to write exceeds the bucket capacity")
                .put("X-Topic", topic)
                .put("X-BucketCapacity", maximum)
                .toBuffer();
    }

    // returns the smallest capacity of the bucket
    private static long getBucketCapacity(Bandwidth[] bandwidths, RoutingContext routingContext) {
        long capacity = Long.MAX_VALUE;
        for (Bandwidth bandwidth : bandwidths) {
            long period = bandwidth.getRefillPeriodNanos();
            if (period == TimeUnit.SECONDS.toNanos(1)) {
                routingContext.response().putHeader("X-Limit-Seconds", Long.toString(bandwidth.getCapacity()));
            } else if (period == TimeUnit.MINUTES.toNanos(1)) {
                routingContext.response().putHeader("X-Limit-Minutes", Long.toString(bandwidth.getCapacity()));
            } else if (period == TimeUnit.HOURS.toNanos(1)) {
                routingContext.response().putHeader("X-Limit-Hours", Long.toString(bandwidth.getCapacity()));
            }
            capacity = Long.min(capacity, bandwidth.getCapacity());            
        }
        return capacity;
    }
    
    private static String getBucketName(RateLimitSingleLimit limit, String topic, Map<String, String> headers, 
            String ipAddress, boolean includeTopic) {
        String aggregationHeaderName = limit.getHeaderAggregation();
        ipAddress = limit.getIgnoreIp() ? IGNORE_IP : ipAddress;
        String aggregationHeaderValue = aggregationHeaderName.length() == 0 ? IGNORE_HEADER : headers.get(aggregationHeaderName);
        String temp = ipAddress + aggregationHeaderValue;
        return includeTopic ? temp + "_" + topic : temp;
    }

    private static int getRateLimitingStrategy(RateLimitingPolicy rateLimitingPolicy, String topic) {
        if (rateLimitingPolicy.getLimits().containsKey(topic) && rateLimitingPolicy.getLimits().get(topic).isGlobal()) {
            return TOPIC_GLOBAL;
        } else if (rateLimitingPolicy.getLimits().containsKey(topic)) {
            return TOPIC_LOCAL;
        } else if (rateLimitingPolicy.getDefaultLimitInstance() != null && rateLimitingPolicy.getDefaultLimitInstance().isGlobal()) {
            return DEFAULT_GLOABAL;
        } else if (rateLimitingPolicy.getDefaultLimitInstance() != null) {
            return DEFAULT_LOCAL;
        }
        return -1;
    }

    private static long getNumberOfTokens(RoutingContext routingContext, HttpSourceBridgeEndpoint<?, ?> bridge, String topic) {
        MessageConverter<?, ?, Buffer, Buffer> messageConverter = bridge.getMessageConverter();
        if (messageConverter == null) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return -1;
        }
        return (long) bridge.getMessageConverter().toKafkaRecords(topic, null, routingContext.getBody()).size();
    }
}
