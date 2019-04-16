package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.bridge.http.APImodels.Consumer;
import io.strimzi.kafka.bridge.http.APImodels.Offsets;
import io.strimzi.kafka.bridge.http.APImodels.Partition;
import io.strimzi.kafka.bridge.http.APImodels.Partitions;
import io.strimzi.kafka.bridge.http.APImodels.Record;
import io.strimzi.kafka.bridge.http.APImodels.Records;
import io.strimzi.kafka.bridge.http.APImodels.TopicMetadata;
import io.strimzi.kafka.bridge.http.APImodels.Topics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Encoding;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;

public class EndPoints {

    // POST endpoints

    @Operation(summary = "Send a record to topic",
          method = "POST",
          operationId = "topics/:topic_name",
          description = "Send a record to topic",
          tags = {
                  "Topics"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "topic_name",
                          required = true, description = "The name of topic where send the records", schema = @Schema(type = "string"))
          },
          requestBody = @RequestBody(
                  description = "JSON object of records",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "records", example = "{'records':[" +
                                  "{" +
                                  "'key':'Strimzi'," +
                                  "'value':'is awesome'" +
                                  "}," +
                                  "{" +
                                  "'value':'Answer for life is'," +
                                  "'partition':'42'" +
                                  "}" +
                                  "]}", implementation = Records.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "400", description = "Unprocessable request."),
                  @ApiResponse(responseCode = "40401", description = "Topic not found."),
                  @ApiResponse(responseCode = "40402", description = "Partition not found.")

          }
    )
    public void sendToTopic(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Records.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }

    }

    @Operation(summary = "Send a record to specified partition of topic",
          description = "Send a record to specified partition of topic",
          method = "POST",
          operationId = "topics/:topic_name/partitions/:partition",
          tags = {
                  "Partitions"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "topic_name",
                          required = true, description = "The name of topic where send the records", schema = @Schema(type = "string")),
                  @Parameter(in = ParameterIn.PATH, name = "partition",
                          required = true, description = "The partition of topic where send the records", schema = @Schema(type = "integer"))
          },
          requestBody = @RequestBody(
                  description = "JSON object of records",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "records", example = "{'records':[" +
                                  "{" +
                                  "'key':'Strimzi'," +
                                  "'value':'is awesome'" +
                                  "}," +
                                  "{" +
                                  "'value':'Answer for life is'," +
                                  "'partition':'42'" +
                                  "}" +
                                  "]}",
                                  implementation = Records.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "400", description = "Unprocessable request."),
                  @ApiResponse(responseCode = "40401", description = "Topic not found."),
                  @ApiResponse(responseCode = "40402", description = "Partition not found.")

          }
    )
    public void sendToTopicPartition(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Records.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Create a new consumer instance in the consumer group",
          description = "Create a new consumer instance in the consumer group",
          method = "POST",
          operationId = "consumers/:group_name",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group to join", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of consumer",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "consumer", example = "{'name':'userName'}",
                                  implementation = Consumer.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40902", description = "Consumer already exists")
          }
    )
    public void createConsumerInGroup(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Consumer.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Commit a list of offsets for the consumer",
          description = "Commit a list of offsets for the consumer",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/offsets",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found")
          }
    )
    public void commitOffsets(RoutingContext context)
    {
      context.response()
              .setStatusCode(200).end();
    }

    @Operation(summary = "Subscribe to the given list of topics",
          description = "Subscribe to the given list of topics",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/subscription",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of topics",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "topics", example = "{'topics':[" +
                                  "'topic1'," +
                                  "'topic2'" +
                                  "]}",
                                  implementation = Topics.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
                  @ApiResponse(responseCode = "40903", description = "Subscription to topics, partitions and pattern are mutually exclusive")

          }
    )
    public void subscribeTopics(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Topics.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Manually assign a list of partitions to this consumer",
          description = "Manually assign a list of partitions to this consumer",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/assignments",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of partitions",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "partitions", example = "{'partitions':[" +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 0" +
                                  "}," +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 1" +
                                  "}" +
                                  "]}",
                                  implementation = Partitions.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
                  @ApiResponse(responseCode = "40903", description = "Subscription to topics, partitions and pattern are mutually exclusive")

          }
    )
    public void overrideOffset(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Partitions.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Overrides the fetch offsets that the consumer will use for the next set of records to fetch",
          description = "Overrides the fetch offsets that the consumer will use for the next set of records to fetch",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/positions",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of partitions",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "partitions", example = "{\n" +
                                  "  \"offsets\": [\n" +
                                  "    {\n" +
                                  "      \"topic\": \"test\",\n" +
                                  "      \"partition\": 0,\n" +
                                  "      \"offset\": 20\n" +
                                  "    },\n" +
                                  "    {\n" +
                                  "      \"topic\": \"test\",\n" +
                                  "      \"partition\": 1,\n" +
                                  "      \"offset\": 30\n" +
                                  "    }\n" +
                                  "  ]\n" +
                                  "}",
                                  implementation = Offsets.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
          }
    )
    public void assignTopicsToConsumer(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Partitions.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Seek to the first offset for each of the given partitions",
          description = "Seek to the first offset for each of the given partitions",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/positions/beginning",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of partitions",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "partitions", example = "{'partitions':[" +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 0" +
                                  "}," +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 1" +
                                  "}" +
                                  "]}",
                                  implementation = Partitions.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
          }
    )
    public void seekFirstOffset(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Partitions.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    @Operation(summary = "Seek to the last offset for each of the given partitions",
          description = "Seek to the last offset for each of the given partitions",
          method = "POST",
          operationId = "consumers/:group_name/instances/:instance/positions/end",
          tags = {
                  "Consumers"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "group_name",
                          required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                  @Parameter(in = ParameterIn.PATH, name = "instance",
                          required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
          },
          requestBody = @RequestBody(
                  description = "JSON object of partitions",
                  content = @Content(
                          mediaType = "application/json",
                          encoding = @Encoding(contentType = "application/json"),
                          schema = @Schema(name = "partitions", example = "{'partitions':[" +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 0" +
                                  "}," +
                                  "{" +
                                  "'topic' : 'topicA'," +
                                  "'partition' : 1" +
                                  "}" +
                                  "]}",
                                  implementation = Partitions.class)
                  ),
                  required = true
          ),
          responses = {
                  @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
          }
    )
    public void seekLastOffset(RoutingContext context)
    {
    final JsonObject product = context.getBodyAsJson();

    ObjectMapper pojoMapper = new ObjectMapper();
    pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Object sanitised = new Object();

    try {
      // We check to make sure that the payload matches our schema definition
      sanitised = pojoMapper.readValue(product.encode(), Partitions.class);

      // add code would go here

      context.response()
              .setStatusCode(200).end();


    } catch (IOException e) {
      // The exception will usually be where an item in the payload does not match the Records schema.
      context.response()
              .setStatusCode(400)
              .end(new JsonObject().put("error", e.getMessage()).encode());
    }
    }

    // GET

    @Operation(summary = "Get a list of Kafka topics.",
            description = "Get a list of Kafka topics.",
            method = "GET",
            operationId = "topics",
          tags = {
                  "Topics"
          },
          responses = {
                  @ApiResponse(responseCode = "200", description = "OK",
                          content = @Content(
                                  mediaType = "application/json",
                                  encoding = @Encoding(contentType = "application/json"),
                                  schema = @Schema(name = "topics", example =
                                          "[\"topic1\",\"topic2\"]",
                                          implementation = Topics.class)
                          )
                  ),
                  @ApiResponse(responseCode = "40401", description = "Topic not found")
          }
    )
    public void listTopics(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("['topic1', 'topic2']");
    }

    @Operation(summary = "Get metadata about a specific topic",
            description = "Get metadata about a specific topic",
            method = "GET",
            operationId = "topics/:topic_name",
          tags = {
                  "Topics"
          },
          parameters = {
                  @Parameter(in = ParameterIn.PATH, name = "topic_name",
                          required = true, description = "Name of the topic to get metadata about", schema = @Schema(type = "string"))
          },
          responses = {
                  @ApiResponse(responseCode = "200", description = "OK",
                          content = @Content(
                                  mediaType = "application/json",
                                  encoding = @Encoding(contentType = "application/json"),
                                  schema = @Schema(name = "topicmetadata", example = "{\n" +
                                          "  \"name\": \"test\",\n" +
                                          "  \"configs\": {\n" +
                                          "     \"cleanup.policy\": \"compact\"\n" +
                                          "  },\n" +
                                          "  \"partitions\": [\n" +
                                          "    {\n" +
                                          "      \"partition\": 1,\n" +
                                          "      \"leader\": 1,\n" +
                                          "      \"replicas\": [\n" +
                                          "        {\n" +
                                          "          \"broker\": 1,\n" +
                                          "          \"leader\": true,\n" +
                                          "          \"in_sync\": true\n" +
                                          "        },\n" +
                                          "        {\n" +
                                          "          \"broker\": 2,\n" +
                                          "          \"leader\": false,\n" +
                                          "          \"in_sync\": true\n" +
                                          "        }\n" +
                                          "      ]\n" +
                                          "    },\n" +
                                          "    {\n" +
                                          "      \"partition\": 2,\n" +
                                          "      \"leader\": 2,\n" +
                                          "      \"replicas\": [\n" +
                                          "        {\n" +
                                          "          \"broker\": 1,\n" +
                                          "          \"leader\": false,\n" +
                                          "          \"in_sync\": true\n" +
                                          "        },\n" +
                                          "        {\n" +
                                          "          \"broker\": 2,\n" +
                                          "          \"leader\": true,\n" +
                                          "          \"in_sync\": true\n" +
                                          "        }\n" +
                                          "      ]\n" +
                                          "    }\n" +
                                          "  ]\n" +
                                          "}",
                                          implementation = TopicMetadata.class)
                          )
                  ),
                  @ApiResponse(responseCode = "40401", description = "Topic not found")
          }
    )
    public void getTopicMetadata(RoutingContext context)
    {
    context.response()
            .setStatusCode(200)
            .end("\"" + context.pathParam("topic_name") + "\"");
    }

    @Operation(summary = "Get a list of partitions for the topic",
            description = "Get a list of partitions for the topic",
            method = "GET",
            operationId = "topics/:topic_name/partitions",
            tags = {
                    "Partitions"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "topic_name",
                            required = true, description = "Name of the topic to get metadata about", schema = @Schema(type = "string"))
            },
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "partitions", example = "{\n" +
                                            "  \"partitions\": [\n" +
                                            "    {\n" +
                                            "      \"partition\": 1,\n" +
                                            "      \"leader\": 1,\n" +
                                            "      \"replicas\": [\n" +
                                            "        {\n" +
                                            "          \"broker\": 1,\n" +
                                            "          \"leader\": true,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        },\n" +
                                            "        {\n" +
                                            "          \"broker\": 2,\n" +
                                            "          \"leader\": false,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        }\n" +
                                            "      ]\n" +
                                            "    },\n" +
                                            "    {\n" +
                                            "      \"partition\": 2,\n" +
                                            "      \"leader\": 2,\n" +
                                            "      \"replicas\": [\n" +
                                            "        {\n" +
                                            "          \"broker\": 1,\n" +
                                            "          \"leader\": false,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        },\n" +
                                            "        {\n" +
                                            "          \"broker\": 2,\n" +
                                            "          \"leader\": true,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        }\n" +
                                            "      ]\n" +
                                            "    }\n" +
                                            "  ]\n" +
                                            "}",
                                            implementation = Partitions.class)
                            )
                    ),
                    @ApiResponse(responseCode = "40401", description = "Topic not found")
            }
    )
    public void getTopicPartitions(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("topic_name") + "\"");
    }

    @Operation(summary = "Get metadata about a single partition in the topic",
            description = "Get metadata about a single partition in the topic",
            method = "GET", operationId = "topics/:topic_name/partitions/:partition_id",
            tags = {
                    "Partitions"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "topic_name",
                            required = true, description = "Name of the topic", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.PATH, name = "partition_id",
                            required = true, description = "ID of the partition to inspect", schema = @Schema(type = "integer"))
            },
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "partition", example = "{\n" +
                                            "      \"partition\": 1,\n" +
                                            "      \"topic\": \"topic1\",\n" +
                                            "      \"leader\": 1,\n" +
                                            "      \"replicas\": [\n" +
                                            "        {\n" +
                                            "          \"broker\": 1,\n" +
                                            "          \"leader\": true,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        },\n" +
                                            "        {\n" +
                                            "          \"broker\": 2,\n" +
                                            "          \"leader\": false,\n" +
                                            "          \"in_sync\": true\n" +
                                            "        }\n" +
                                            "       ]\n" +
                                            "}",
                                            implementation = Partition.class)
                            )
                    ),
                    @ApiResponse(responseCode = "40401", description = "Topic not found"),
                    @ApiResponse(responseCode = "40402", description = "Partition not found")
            }
    )
    public void getTopicPartition(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("topic_name") + "/" +  context.pathParam("partition_id") + "\"");
    }

    @Operation(summary = "Get the last committed offsets for the given partitions",
            description = "Get the last committed offsets for the given partitions",
            method = "GET",
            operationId = "consumers/:group_name/instances/:instance/offsets",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string"))
            },
            requestBody = @RequestBody(
                    description = "JSON object of product",
                    content = @Content(
                            mediaType = "application/json",
                            encoding = @Encoding(contentType = "application/json"),
                            schema = @Schema(name = "partitions", example = "{\n" +
                                    "  \"partitions\": [\n" +
                                    "    {\n" +
                                    "      \"topic\": \"test\",\n" +
                                    "      \"partition\": 0\n" +
                                    "    },\n" +
                                    "    {\n" +
                                    "      \"topic\": \"test\",\n" +
                                    "      \"partition\": 1\n" +
                                    "    }\n" +
                                    "\n" +
                                    "  ]\n" +
                                    "}", implementation = Partitions.class)
                    ),
                    required = true),
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "offsets", example = "{\n" +
                                            "\"offsets\":\n" +
                                            " [\n" +
                                            "  {\n" +
                                            "    \"topic\": \"test\",\n" +
                                            "    \"partition\": 0,\n" +
                                            "    \"offset\": 21,\n" +
                                            "    \"metadata\":\"\"\n" +
                                            "  },\n" +
                                            "  {\n" +
                                            "    \"topic\": \"test\",\n" +
                                            "    \"partition\": 1,\n" +
                                            "    \"offset\": 31,\n" +
                                            "    \"metadata\":\"\"\n" +
                                            "  }\n" +
                                            " ]\n" +
                                            "}",
                                            implementation = Offsets.class)
                            )
                    ),
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found")
            }
    )
    public void lastOffsets(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("group_name") + "/" +  context.pathParam("instance") + "\"");
    }

    @Operation(summary = "Get the current subscribed list of topics",
            description = "Get the current subscribed list of topics",
            method = "GET",
            operationId = "consumers/:group_name/instances/:instance/subscription",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string"))
            },
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "topics", example = "{\n" +
                                            "  \"topics\": [\n" +
                                            "    \"topic1\",\n" +
                                            "    \"topic2\"\n" +
                                            "  ]\n" +
                                            "}",
                                            implementation = Topics.class)
                            )
                    ),
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
            }
    )
    public void subscribedTopics(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("group_name") + "/" +  context.pathParam("instance") + "\"");
    }

    @Operation(summary = "Get the list of partitions currently manually assigned to this consumer",
            description = "Get the list of partitions currently manually assigned to this consumer",
            method = "GET",
            operationId = "consumers/:group_name/instances/:instance/assignments",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string"))
            },
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "partitions", example = "{\n" +
                                            "  \"partitions\": [\n" +
                                            "    {\n" +
                                            "      \"topic\": \"test\",\n" +
                                            "      \"partition\": 0\n" +
                                            "    },\n" +
                                            "    {\n" +
                                            "      \"topic\": \"test\",\n" +
                                            "      \"partition\": 1\n" +
                                            "    }\n" +
                                            "  ]\n" +
                                            "}",
                                            implementation = Partitions.class)
                            )
                    ),
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
            }
    )
    public void manuallyAssignedPartitions(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("group_name") + "/" +  context.pathParam("instance") + "\"");
    }

    @Operation(summary = "Fetch data for the topics or partitions specified using one of the subscribe/assign APIs",
            description = "Fetch data for the topics or partitions specified using one of the subscribe/assign APIs",
            method = "GET",
            operationId = "consumers/:group_name/instances/:instance/records",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
                    @Parameter(in = ParameterIn.QUERY, name = "timeout",
                            description = "Maximum amount of milliseconds the REST proxy will spend fetching records", schema = @Schema(type = "integer")),
                    @Parameter(in = ParameterIn.QUERY, name = "max_bytes",
                            description = "The maximum number of bytes of unencoded keys and values that should be included in the response", schema = @Schema(type = "integer"))
            },
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK",
                            content = @Content(
                                    mediaType = "application/json",
                                    encoding = @Encoding(contentType = "application/json"),
                                    schema = @Schema(name = "partitions", example = "[\n" +
                                            "  {\n" +
                                            "    \"topic\": \"test\",\n" +
                                            "    \"key\": \"a2V5\",\n" +
                                            "    \"value\": \"Y29uZmx1ZW50\",\n" +
                                            "    \"partition\": 1,\n" +
                                            "    \"offset\": 100\n" +
                                            "  },\n" +
                                            "  {\n" +
                                            "    \"topic\": \"test\",\n" +
                                            "    \"key\": \"a2V5\",\n" +
                                            "    \"value\": \"a2Fma2E=\",\n" +
                                            "    \"partition\": 2,\n" +
                                            "    \"offset\": 101\n" +
                                            "  }\n" +
                                            "]",
                                            implementation = Record[].class)
                            )
                    ),
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found"),
                    @ApiResponse(responseCode = "40601", description = "Consumer format does not match the embedded format requested by the Accept header.")
            }
    )
    public void fetchData(RoutingContext context)
    {
        context.response()
                .setStatusCode(200)
                .end("\"" + context.pathParam("group_name") + "/" +  context.pathParam("instance") + " " +
                        context.queryParam("timeout") + " " + context.queryParam("max_bytes"));
    }


    // DELETE


    @Operation(summary = "Destroy the consumer instance",
            description = "Destroy the consumer instance",
            method = "DELETE",
            operationId = "consumers/:group_name/instances/:instance",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
            },
            responses = {
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found")
            }
    )
    public void destroyConsumer(RoutingContext context)
    {
        context.response()
                .setStatusCode(200).end();
    }


    @Operation(summary = "Unsubscribe from topics currently subscribed",
            description = "Unsubscribe from topics currently subscribed",
            method = "DELETE",
            operationId = "consumers/:group_name/instances/:instance/subscription",
            tags = {
                    "Consumers"
            },
            parameters = {
                    @Parameter(in = ParameterIn.PATH, name = "group_name",
                            required = true, description = "The name of the consumer group", schema = @Schema(type = "string")),

                    @Parameter(in = ParameterIn.PATH, name = "instance",
                            required = true, description = "The ID of the consumer instance", schema = @Schema(type = "string")),
            },
            responses = {
                    @ApiResponse(responseCode = "40403", description = "Consumer instance not found")
            }
    )
    public void unsubscribeFromTopics(RoutingContext context)
    {
        context.response()
                .setStatusCode(200).end();
    }
}
