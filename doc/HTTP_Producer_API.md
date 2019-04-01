# Producer API

## Produce message

Producer API allows you to send message to a topic, optionally specifying key or partition.

* Endpoint : `http://hostip:port/topics/{topic_name}`
* Request method : `POST`
* Path params : `topic_name`
* Request body params : `partition [optional]` , `key [optional]` , `value`

Request body should be in JSON format.

Sample request for sending three records:

* specifying a key
* specifying a partition
* just with message value

```
POST /topics/kafka-bridge HTTP/1.1
Host: localhost:8080
Content-Type: application/application/json
{
    "records": [
        {
            "key": "my-key",
            "value": "Hi this is kafka-bridge (with key)"
        },
        {
            "value": "Hi this is kafka-bridge (with partition)",
            "partition": 1
        },
        {
            "value": "Hi this is kafka-bridge"
        }
    ]
}
```

Response is also a JSON object containing metadata of the produced messages.

Sample response

```
HTTP/1.1 200 OK
Content-Type: application/json

{
    "offsets": [
        {
            "partition": 2,
            "offset": 0
        },
        {
            "partition": 1,
            "offset": 0
        },
        {
            "partition": 2,
            "offset": 1
        }
    ]
}
```

In case of errors the corresponding "offset" JSON contains "error_code" and "error" fields.