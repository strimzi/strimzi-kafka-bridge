# Producer API

## Produce message

Producer API allows you to send message to a topic, optionally specifying key or partition.

* Endpoint : `http://hostip:port/topics/{topic_name}`
* Request method : `POST`
* Path params : `topic_name`
* Request body params : `partition [optional]` , `key [optional]` , `value`

Request body should be in JSON format.

Sample request specifying a key:

```
POST /topics/kafka-bridge HTTP/1.1
Host: localhost:8080
Content-Type: application/application/json
{"key":"my-key","value":"Hi this is kafka-bridge"}
```

Sample request specifying a partition:

```
POST /topics/kafka-bridge HTTP/1.1
Host: localhost:8080
Content-Type: application/application/json
{"partition":1,"value":"Hi this is kafka-bridge"}
```

Response is also a JSON object containing delivery report and metadata of the produced message.

Sample response

```
HTTP/1.1 200 OK
Content-Type: application/json

{"status":"Accepted","topic":"kafka-bridge","partition": 1,"offset": 1}
```

In case of errors while producing message, the `status` will be sent as `Rejected`.
