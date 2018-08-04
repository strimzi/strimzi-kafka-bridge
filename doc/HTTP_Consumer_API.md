# Consumer API
#### Consumer API provides allows you to create a consumer in a consumer group and consume messages from topics and partitions. For each type of request only `JSON` format is supported.

* ### Creating a consumer
    * #### Create a new consumer instance in the consumer group. Optionally specifying name of consumer instance. If name is not specified a random name will be assigned.
        ##### Endpoint : `http://BRIDGE_HOST:BRIDGE_PORT/consumers/{group-id}`
        ##### Request method : `POST`
        ##### Path params : `group-id`
        ##### Request body params : `name [optional]`

    * #### Sample request
        ```
        POST /consumer/group-1 HTTP/1.1
        Host: localhost:8080
        Content-Type: application/application/json
        {"name":"kafkaconsumer123"}
        ```
    * #### Response format
        ##### Response is a json object containing instance_id and base_uri of the consumer created. This base_uri should be used to make subsequent requests for other operations like subscribing and consuming.

    * #### Sample response
        ```
        HTTP/1.1 200 OK
        Content-Type: application/json
        {"instance_id":"kafkaconsumer123","base_ur":"http://BRIDGE_HOST:BRIDGE_PORT/consumers/group-1/instances/kafkaconsumer123}
        ```
        
* ### Subscribing to a topic
    * #### subscribe to a topic. Optionally specifying `partition` and `offset`. If a partition is not specified explicitly one will be assigned automatically by the bridge. If an offset is specified explicitly the consumer will `seek()` to that offset.
        ##### Endpoint : `http://BRIDGE_HOST:BRIDGE_PORT/consumers/{group-id}/instances/{instance_id}/subscription`
        ##### Request method : `POST`
        ##### Path params : `group-id`, `instance_id`
        ##### Request body params : `topic`, `partition [optional]`, `offset [optional]`

    * #### Sample request
        ```
        POST /consumer/group-1/instances/kafkaconsumer123/subscription HTTP/1.1
        Host: localhost:8080
        Content-Type: application/application/json
        {"topic":"mytopic","partition":1}
        ```
    * #### Response format
        ##### Response is a json object containing `subscription_status`.

    * #### Sample response
        ```
        HTTP/1.1 200 OK
        Content-Type: application/json
        {"subscription_status":"subscribed"}
        ```
        
* ### Consuming records
    * #### consuming records from the kafka. The consumer should be subscribe to a topic to consume any records. `timeout` can be explicitly specified in the request header.
        ##### Endpoint : `http://BRIDGE_HOST:BRIDGE_PORT/consumers/{group-id}/instances/{instance_id}/records`
        ##### Request method : `GET`
        ##### Path params : `group-id`, `instance_id`
        ##### Request header : `timeout`

    * #### Sample request
        ```
        GET /consumer/group-1/instances/kafkaconsumer123/records HTTP/1.1
        Host: localhost:8080
        Content-Type: application/application/json, timeout = 1000
        ```
    * #### Response format
        ##### Response is a json array containing records. each record is a json object itself with all the metadata.

    * #### Sample response
        ```
        HTTP/1.1 200 OK
        Content-Type: application/json
        [{"topic":"mytopic","key":null,"partition":1,"value":"Hi this is kafka bridge","offset":0},
        {"topic":"mytopic","key":null,"partition":1,"value":"Kafka is awesome","offset":1}]
        ```

* ### Deleting a consumer instance
    * #### Delete a consumer instance.
        ##### Endpoint : `http://BRIDGE_HOST:BRIDGE_PORT/consumers/{group-id}/instances/{instance_id}`
        ##### Request method : `DELETE`
        ##### Path params : `group-id`, `instance_id`

    * #### Sample request
        ```
        DELETE /consumer/group-1/instances/kafkaconsumer123 HTTP/1.1
        Host: localhost:8080
        Content-Type: application/application/json
        ```
    * #### Response format
        ##### Response is a json object containing `status` and `instance_id`.

    * #### Sample response
        ```
        HTTP/1.1 200 OK
        Content-Type: application/json
        {"instance_id":"kafkaconsumer123","status":"deleted"}
        ```
        
* ### Commiting offsets
    * #### commit a list of offsets. specifying `topic`, `partition`, `offset` is compulsory for each record to be commit.
        ##### Endpoint : `http://BRIDGE_HOST:BRIDGE_PORT/consumers/{group-id}/instances/{instance_id}/offsets`
        ##### Request method : `POST`
        ##### Path params : `group-id`, `instance_id`
        ##### Request body params : `offsets`, `offsets[i]`, `offsets[i].topic`, `offsets[i].partition`, `offsets[i].offset`

    * #### Sample request
        ```
        POST /consumer/group-1/instances/kafkaconsumer123/offsets HTTP/1.1
        Host: localhost:8080
        Content-Type: application/application/json
        {"offsets": [{"topic": "mytopic","partition": 1,"offset": 0},
        {"topic": "mytopic","partition": 1,"offset": 1}]}
        ```
    * #### Response format
        ##### upon succesful commit the respnse will be empty with HTTP status code `200`

    * #### Sample response
        ```
        HTTP/1.1 200 OK
        Content-Type: application/json
        ```
