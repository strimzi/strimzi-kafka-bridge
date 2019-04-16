/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import io.strimzi.kafka.bridge.http.generator.OpenApiRoutePublisher;

import io.strimzi.kafka.bridge.http.EndPoints;
import io.strimzi.kafka.bridge.http.generator.Required;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.reflect.Field;
import java.lang.reflect.Type;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SwaggerVerticle extends AbstractVerticle {
    public static final String APPLICATION_JSON = "application/json";
    private static final int PORT = 8081;
    private static final String HOST = "localhost";
    private HttpServer server;
    private static final Logger log = LoggerFactory.getLogger(SwaggerVerticle.class);

    private EndPoints endPoints;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        log.info("Starting Swagger verticle...");
        endPoints = new EndPoints();
        this.bindHttpServer(startFuture);
    }

    @Override
    public void stop(Future<Void> future) {
        if (server == null) {
            future.complete();
            return;
        }
        server.close(future.completer());
    }

    private void bindHttpServer(Future<Void> startFuture) {


        this.server = this.vertx.createHttpServer(createOptions())
                //.connectionHandler(this::processConnection)
                .requestHandler(configurationRouter()::accept)
                .listen(httpServerAsyncResult -> {
                    if (httpServerAsyncResult.succeeded()) {
                        log.info("Swagger UI started and listening on port {}", httpServerAsyncResult.result().actualPort());
                        startFuture.complete();
                    } else {
                        log.error("Error starting Swagger UI", httpServerAsyncResult.cause());
                        startFuture.fail(httpServerAsyncResult.cause());
                    }
                });
    }

    private HttpServerOptions createOptions() {
        HttpServerOptions options = new HttpServerOptions();
        options.setHost(HOST);
        options.setPort(PORT);
        return options;
    }

    private Router configurationRouter() {
        Router router = Router.router(vertx);
        router.route().consumes(APPLICATION_JSON);
        router.route().produces(APPLICATION_JSON);
        router.route().handler(BodyHandler.create());

        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("auth");
        allowedHeaders.add("Content-Type");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.PATCH);
        allowedMethods.add(HttpMethod.PUT);

        router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));

        router.route().handler(context -> {
            context.response().headers().add(CONTENT_TYPE, APPLICATION_JSON);
            context.next();
        });
        router.route().failureHandler(ErrorHandler.create(true));

        router.post("/topics/:topic_name").handler(endPoints::sendToTopic);
        router.post("/topics/:topic_name/partitions/:partition").handler(endPoints::sendToTopicPartition);

        router.post("/consumers/:group_name").handler(endPoints::createConsumerInGroup);
        router.post("/consumers/:group_name/instances/:instance/offsets").handler(endPoints::commitOffsets);
        router.post("/consumers/:group_name/instances/:instance/subscription").handler(endPoints::subscribeTopics);
        router.post("/consumers/:group_name/instances/:instance/assignments").handler(endPoints::assignTopicsToConsumer);
        router.post("/consumers/:group_name/instances/:instance/positions").handler(endPoints::overrideOffset);
        router.post("/consumers/:group_name/instances/:instance/positions/beginning").handler(endPoints::seekFirstOffset);
        router.post("/consumers/:group_name/instances/:instance/positions/end").handler(endPoints::seekLastOffset);


        router.get("/topics").handler(endPoints::listTopics);
        router.get("/topics/:topic_name").handler(endPoints::getTopicMetadata);
        router.get("/topics/:topic_name/partitions").handler(endPoints::getTopicPartitions);
        router.get("/topics/:topic_name/partitions/:partition_id").handler(endPoints::getTopicPartition);

        router.get("/consumers/:group_name/instances/:instance/offsets").handler(endPoints::lastOffsets);
        router.get("/consumers/:group_name/instances/:instance/subscription").handler(endPoints::subscribedTopics);
        router.get("/consumers/:group_name/instances/:instance/assignments").handler(endPoints::manuallyAssignedPartitions);
        router.get("/consumers/:group_name/instances/:instance/records").handler(endPoints::fetchData);

        router.delete("/consumers/:group_name/instances/:instance").handler(endPoints::destroyConsumer);
        router.delete("/consumers/:group_name/instances/:instance/subscription").handler(endPoints::unsubscribeFromTopics);


        OpenAPI openAPIDoc = OpenApiRoutePublisher.publishOpenApiSpec(
                router,
                "spec",
                "Strimzi Kafka Bridge API documentation",
                "1.0.0",
                "http://" + HOST + ":" + PORT + "/"
        );

        /* Tagging section. This is where we can group end point operations; The tag name is then used in the end point annotation
         */
        openAPIDoc.addTagsItem( new io.swagger.v3.oas.models.tags.Tag().name("Partitions").description("Provides per-partition metadata. Also allows you to consume and produce messages to single partition using `GET` and `POST` requests"));
        openAPIDoc.addTagsItem( new io.swagger.v3.oas.models.tags.Tag().name("Topics").description("The topics resource provides information about the topics in your Kafka cluster. Also lets you produce messages by making POST requests to specific topics"));
        openAPIDoc.addTagsItem( new io.swagger.v3.oas.models.tags.Tag().name("Consumers").description("Manage consumers"));


        // Generate the SCHEMA section of Swagger, using the definitions in the Model folder
        ImmutableSet<ClassPath.ClassInfo> modelClasses = getClassesInPackage("io.strimzi.kafka.bridge.http.APImodels");

        Map<String, Object> map = new HashMap<String, Object>();

        for(ClassPath.ClassInfo modelClass : modelClasses){

            Field[] fields = FieldUtils.getFieldsListWithAnnotation(modelClass.load(), Required.class).toArray(new
                    Field[0]);
            List<String> requiredParameters = new ArrayList<String>();

            for(Field requiredField : fields){
                requiredParameters.add(requiredField.getName());
            }

            fields = modelClass.load().getDeclaredFields();

            for (Field field : fields) {
                mapParameters(field, map);
            }

            openAPIDoc.schema(modelClass.getSimpleName(),
                    new Schema()
                            .title(modelClass.getSimpleName())
                            .type("object")
                            .required(requiredParameters)
                            .properties(map)
            );

            map = new HashMap<String, Object>();
        }
        //

        // Serve the Swagger JSON spec out on /swagger
        router.get("/swagger").handler(res -> {
            res.response()
                    .setStatusCode(200)
                    .end(Json.pretty(openAPIDoc));
        });

        // Serve the Swagger UI out on /doc/index.html
        router.route("/doc/*").handler(StaticHandler.create().setCachingEnabled(false).setWebRoot("webroot/node_modules/swagger-ui-dist"));

        return router;
    }


    private void mapParameters(Field field, Map<String, Object> map) {
        Class type = field.getType();
        Class componentType = field.getType().getComponentType();

        if (isPrimitiveOrWrapper(type)) {
            Schema primitiveSchema = new Schema();
            primitiveSchema.type(field.getType().getSimpleName());
            map.put(field.getName(), primitiveSchema);
        } else {
            HashMap<String, Object> subMap = new HashMap<String, Object>();

            if(isPrimitiveOrWrapper(componentType)){
                HashMap<String, Object> arrayMap = new HashMap<String, Object>();
                arrayMap.put("type", componentType.getSimpleName() + "[]");
                subMap.put("type", arrayMap);
            } else {
                subMap.put("$ref", "#/components/schemas/" + componentType.getSimpleName());
            }

            map.put(field.getName(), subMap);
        }
    }

    private Boolean isPrimitiveOrWrapper(Type type){
        return type.equals(Double.class) ||
                type.equals(Float.class) ||
                type.equals(Long.class) ||
                type.equals(Integer.class) ||
                type.equals(Short.class) ||
                type.equals(Character.class) ||
                type.equals(Byte.class) ||
                type.equals(Boolean.class) ||
                type.equals(String.class) ||
                type.equals(Object.class); //TODO some schemas are fault-ish without this line
    }

    public ImmutableSet<ClassPath.ClassInfo> getClassesInPackage(String pckgname) {
        try {
            ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
            ImmutableSet<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(pckgname);
            return classes;

        } catch (Exception e) {
            return null;
        }
    }
}
