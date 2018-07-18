# Producer API

## Produce message

* To produce message to a topic, optionally specifying key or partition.

##### Endpoint : `http://hostip:port/topic/{topic_name}`

##### Request method : `POST`

##### Path params : `topic_name`

##### Request body params : `partition [optional]` , `key [optional]` , `value`

## Request format

* Request body should be in JSON format.

#### sample request
```
POST /topics/kafka-bridge HTTP/1.1
Host: localhost:8080
Content-Type: application/application/json
{"key":"key01","partition":1,"value":"Hi this is kafka-bridge"}
```

## Response format

* Response is also a json object containing delivery report and metadata of the message produced.

#### sample response

```
HTTP/1.1 200 OK
Content-Type: application/json

{"status":"Accepted","topic":"kafka-bridge","partition": 1,"offset": 1}
```
* in case there occurs an error while producing message the `status` will be sent as `Rejected`