
# 1 CONTENT-BASED ROUTING

### 1.1 Purpose and overview
The purpose of content-based routing is to dynamically determine to which topic do the messages, issued to the bridge in order to be written to the Kafka cluster, belong to. This functionality can be accessed by making POST requests to the `/topics` route. In other words, this is an alternative to making POST requests to the `/topics/{foo}` route in which the topic (`foo` in this case) is not determined by the client but by the bridge itself.

The routing adheres to the rules that the user specifies in a JSON configuration file, which is passed to the bridge in the `.spec.cbrConfigMap` field of the YAML file that generates the bridge. More precisely, in this field the user needs to specify the name of the Kubernetes ConfigMap that contains the JSON configuration - see section 1.4. If the `.spec.cbrConfigMap` field is omitted or some error occurs while fetching the content of the ConfigMap (e.g. a ConfigMap with the provided name does not exist, the JSON configuration does not have the required format, etc.), the functionality is disabled and attempts to make POST requests to the `/topics` route will result in a `400: BAD REQUEST` error.

### 1.2 Writing content based routing configuration files
There are two possibilities for the routing configuration file:

* The first option is for the JSON file to only contain the name of the class, that alone takes care of routing. In such a case, the configuration must contain the following fields:
    1. `custom-class` (required): the name of the class that contains the `getTopic(...)` method that is invoked in order to determine the topic, to which the messages belong to - see section 1.3;
    2. `url` (required): URL with the format `{IP_ADDRESS}:{PORT}/{ROUTE}`, on which the ZIP file that contains all the necessary classes, including the `custom-class` class, can be downloaded.

    An example of such a configuration is the following:
	```
	{
		"custom-class": "com.boris.test.Router",
		"url": "http://192.168.1.14:9090/topRouter.zip"
   	}
	```

	Explanation of the example: the bridge downloads the `topRouter.zip` file, which is available on `http://192.168.1.14:9090/topRouter.zip`, and extracts it. After that, each time the bridge receives a `POST` request issued to the `/topics` route, the `getTopic(...)` method of the `com.boris.test.Router` class is invoked. The method returns a String that determines to which topic should the messages be written to. For example if the method returns `foo`, the messages are written to the `foo` topic (see section 1.4). Note that all the topics should manually be created beforehand;
	
* Alternatively, the JSON file contains (optionally) a set of custom rules (`custom-rules`) and (required) a default topic (`default-topic`). Each custom rule contains, apart from a (required) `topic` field, also (required) a non-empty array of conditions (`conditions`), and in order for the custom rule to be applied, all the condition specified inside of it must be satisfied. A condition has one of the following formats:
  
	1. Contains (required) `header-name`, (optionally) `header-value` and (optionally) `exists`. The first determines the HTTP header, that is going to be considered by the condition, while the second determines the value, expressed as a regular expression, that the value of the header needs to match; `exists` is by default `true`, and should be set to `false` when the condition requires that the `header-name` header should not be present. If `exists` is set to `false`, the `header-value` field **must** be omitted. **BEWARE**: headers without a value are removed by the parser, so if the request contains the header `foo: `, the header is completely ignored;
	2. Contains `custom-class` and `url` - the purpose of these two is identical as above: `url` is the URL with the format `{IP_ADDRESS}:{PORT}/{ROUTE}`, on which the ZIP file that contains all the necessary classes, including the `custom-class` class, can be downloaded. `custom-class` identifies the name of the class, that contains the `isSatisfied(...)` method. This method is invoked each time a request is received and should return `true` if the condition is satisfied and `false` otherwise - see section 1.3.

	In other words, when we wish to consider only the HTTP headers for routing, there is just the need to specify the name and the value of the considered HTTP headers, while if we wish to carry out any other type of routing, we need to provide custom classes.
	Apart from the list of routing rules, the configuration must also contain a `default-topic`, which represents the topic, to which messages should be written to if none of the rules can be applied. The default topic, as well as the topic fields contained in the custom rules, can contain the following properties:
 
	1. `topic-name` (required): name of the topic, to which messages should be routed. The topic is automatically created (if it does not exist already); 
	3. `replicationFactor` (optional): replication factor of the topic. This parameter is going to be considered only if the `topic-name` topic does not exist prior to the launch of the bridge;
	4. `replicationFactor` (optional): number of partitions of the topic. This parameter is going to be considered only if the `topic-name` topic does not exist prior to the launch of the bridge;

	An example of a possible configuration is the following:
	```
	{
		"custom-rules": [{
				"conditions": [
					{
					  "header-name": "X-Custom-Header",
					  "value": ".*(sun|fog|cloud|wind|rain).*"
					}
				],
				"topic": {
					"topic-name": "weather_discussion",
					"replicationFactor": 1,
					"partitionNumber": 1
				}
			}
		],
		"default-topic": {
			"topic-name": "random_discussion",
		  	"replicationFactor": 1,
		  	"partitionNumber": 1
		}
	}
	```

	Explanation of the example: if the HTTP request issued to the `/topics` route contains the `X-Custom-Header` header **and** if the value of this header contains inside of it either of the following words (`sun`, `fog`, `cloud`, `wind`, or `rain`), the message is written to the `weather_discussion` topic. If this topic still needs to be created, it is created with replication factor of one and only one partition. All the other messages, i.e. those without the `X-Custom-Header` header and those whose value of the `X-Custom-Header` header does not contain the above mentioned words, are going to be written to the `random_discussion` topic.

The schema to which the content-based routing configuration file needs to apply is shown in `config/cbr_schema_validation.json`.
	
### 1.3 Custom routing
In the case you specified to use custom classes, you are required to group all the classes, that are necessary for the routing process, inside of a ZIP file and make it available over the network. The Bridge Pods will automatically download the ZIP file, extract its content and use it for routing. Depending on which type of routing you wish to use (see above), you need to specify a class that contains one of the following methods:
* `public boolean isSatisfied(Map<String, String> headers, String body)`: the parameter `body` contains the body of the HTTP message, encoded with the UTF8 encoding. The method must return `true` if the condition is satisfied and `false` otherwise;
* `public String getTopic(Map<String, String> headers, String body)`: the parameter `body` contains the body of the HTTP message, encoded with the UTF8 encoding. The method must return a String representing the name of the topic the message belong to. Note that all topics should be created beforehand. 

### 1.4 Deployment on kubernetes
The following procedure is valid for minikube, but a similar procedure has been tested also on GKE.

1. Start minikube;
2. Follow the steps as in https://strimzi.io/docs/operators/latest/quickstart.html to set up the Strimzi operators, the Kafka cluster, zookeeper (use the YAML files available in `/install/cluster-operator`). **Bewere**: you must deploy the extended version of the cluster controller, available now on GitHub (https://github.com/BorisRado/strimzi-kafka-operator) and DockerHub (borisrado/operator:latest);
3. Create the ConfigMap that contains the JSON configuration. For convenience, the JSON configuration should be the only entry of the ConfigMap. For example you can write the JSON configuration in the `schema_example.json` file and later on create the ConfigMap with the command `kubectl create configmap myCbrConfigMap --from-file schema_example.json -n kafka`. **Beware**: the ConfigMap must be in the same namespace as the Bridge Pods;
4. Deploy the Bridge with two additions: use the `borisrado/kafka-bridge` as the image (`.spec.image`), and set the `.spec.cbrConfigMap` field to contain the name of the ConfigMap that contains the configuration. In the case you created the ConfigMap with the command described in point 3., the YAML file should resemble something like this: ```
	apiVersion: kafka.strimzi.io/v1alpha1
	kind: KafkaBridge
	metadata:
		name: strimzi
		labels:
			app: my-bridge
	spec:
		replicas: 2
		image: borisrado/kafka-bridge
		cbrConfigMap: myCbrConfigMap
		bootstrapServers: my-cluster-kafka-bootstrap:9092
  	http:
		port: 8080
   ```
5. You can now issue requests to the `/topics` route. The topic will be determined by the bridge itself. Other routes are in no way influenced (e.g. sending messages to `/topics/foo` route will still write the messages to the `foo` topic).
When you update the configuration file or change the name of the `.spec.cbrConfigMap` field, the cluster controller will take over and perform a rolling release, so that the existing Pods will be shut down and a set of new ones with the new configuration will be launched.

### 1.5 Possible responses
* `400: BAD REQUEST`: is returned when the client tries to access to the `/topics` route when such option is not enabled (either on purpose or because some error occurred while fetching the ConfigMap);
* All the other responses returned when issuing requests to the `/topics/{foo}` route. Note only that `200: OK` responses contain also the name of the topic, to which the messages have been written to, see the example below:
  ```
	{
		"offsets": [
			{
				"partition":0,
				"offset":2
			},
			{
				"partition":0,
				"offset":3
			}
		],
		"topic": "random_discussion"
	}
  ```
  

# 2 RATE LIMITING
### 2.1 Purpose and overview
The general aim of rate limiting is to prevent the Kafka cluster from being overwhelmed by a large amount of messages, fact that might cause disruptions. The rate limiting check is limited to the process of writing messages to the brokers (i.e. no limits are in place when it comes to reading messages from the Kafka cluster).

The rate limiting adheres to the rules that the user specifies in a JSON configuration file, which is passed to the bridge in the `.spec.rlConfigMap` field of the YAML file that generates the bridge. More precisely, in this field the user needs to specify the name of the Kubernetes ConfigMap that contains the JSON configuration - see section 2.3. If the `.spec.rlConfigMap` field is omitted or some error occurs while fetching the content of the ConfigMap (e.g. a ConfigMap with the provided name does not exist, the JSON configuration does not have the required format, etc.), the functionality is disabled and no limits are in place.

When the limit has not been exceeded by the client, the responses of the bridge contain some of the following headers:
1. `X-Limit-Seconds`: the number of records a client is allowed to write within a second. This header is present only if the number of requests a client is allowed to perform is limited on a seconds basis (e.g. a client is allowed to write maximum 10 messages per second);
2. `X-Limit-Minutes`: the number of records a client is allowed to write within a minute. This header is present only if the number of requests a client is allowed to perform is limited on a minutes basis (e.g. a client is allowed to write maximum 10 messages per minute);
3. `X-Limit-Hours`: the number of records a client is allowed to write within an hour. This header is present only if the number of requests a client is allowed to perform is limited on an hour basis (e.g. a client is allowed to write maximum 10 messages per hour);
4. `X-Limit-Remaining`: the number of records a client is still allowed to write to the topic until the limit is renowned.
When rate limiting is in place, at least one among `X-Limit-Seconds`, `X-Limit-Minutes` and `X-Limit-Hours` is present, while `X-Limit-Remaining` is always present.

Once the limit is reached, `429: TOO MANY REQUESTS` response is returned and the messages have no follow-ups. The responses contain in such cases the `X-Millis-Until-Refill` header, which informs the client about the time, expressed in milliseconds, until when the limit is renowned (until that time, the client is not allowed to write messages to that topic).

The token bucket algorithm is used.

### 2.2 Writing rate-limiting configuration files
The rate-limiting configuration file contains an array of topic-limits. Each topic-limit contains:
* `topic` (optional): name of the topic, to which the limit is being applied. In the case this parameter is missing, the limit is applied to all the topics but the ones with their own limits;
* `strategy` (optional): which strategy to use, either `local` (pods keep private to themselves the data concerning rate limiting, so the outcome of the rate-limiting process varies depending on which pod does handle the request) or `global` (pods share data concerning rate-limiting through a hazelcast cluster). The strategy should be set to `global` when there is the need for the rate limiting check to be precise. By default, this parameter is set to `local`;
* `limits` (required): non-empty array of limits. Each limit can contain the following parameters: 
	* `seconds` (optional): number of messages the client is allowed to write within a second;
	* `minutes`(optional): number of messages the client is allowed to write within a minute;
	* `hours` (optional): number of messages the client is allowed to write within an hour;
	* `group-by-header` (optional): the name of the header whose value uniquely identifies the client. If this parameter is missing, clients are identified by their IP address;
	* `header-name` (optional): name of the header, that contains additional information about the client. For example it might state to which tier does the user belong to. This allows us for example to differentiate between clients in the free and in the premium tier;
	* `header-value` (optional): the value that the `header-name` header needs to match in order for the limit to be applied. This parameter cannot be set if the `header-name` parameter is missing;
	Note that at least one of the `seconds`, `minutes` and `hours` parameters must be present.

A limit is applied to the request if both these conditions are satisfied:
1. the request contains the `header-name` header and its value matches the `header-value` **OR** the limit does not contain the `header-name` parameter;
2. the request contains the `group-by-header` header **OR** if the limit does not contain the `group-by-header` parameter.

The schema to which the rate-limiting rules need to apply is shown in `config/rl_schema_validation.json`.

Here is an example of a possible configuration:
```
[
	{
		"topic": "weather_discussion",
		"limits": [
			{
				"minutes": 3,
				"group-by-header": "X-Consumer-Username",
				"header-name": "X-Consumer-Groups",
				"header-value": "free-acl-tier"
			},
			{
				"minutes": 6,
				"group-by-header": "X-Consumer-Username",
				"header-name": "X-Consumer-Groups",
				"header-value": "premium-acl-tier"
			},
			{
				"minutes": 1
			}
		]
	},
	{
		"strategy": "global",
		"limits": [
			{
				"minutes": 5,
				"group-by-header": "X-Consumer-Username"
			},
			{
				"minutes": 2
			}
		]
	}
]
```
**Explanation of the example**: users that attach the `X-Consumer-Username` header to their request are the authenticated users. If the requests of the authenticated users contains the `X-Consumer-Groups` header and if the value of this header has the value `free-acl-tier`, the client is allowed to write up to 3 records per minute to the `weather_discussion` topic. If the value of the `X-Consumer-Groups` header is `premium-acl-tier`, clients are given a 6 records per minute quota. All the other clients (i.e. the not authenticated ones, the ones that do not attach the `X-Consumer-Groups` header to the request and the ones whose value of the `X-Consumer-Groups` header does not match the two values mentioned above), are given a default, 1 record per minute quota. The local strategy is used. On the other hand, when writing messages to any other topic but the `weather_discussion` one, the global strategy is used; authenticated users are allowed to write up to 5 messages per minute, while all the others are given a default, 2 requests per minute quota.

**Bewere**: the order in which the limits are listed does matter. The most specific limit must be the first, the least specific must be the last. 

### 2.3 Deployment on minikube
The following procedure is valid for minikube, but a similar procedure has been tested also on GKE.

1. Start minikube;
2. Follow the steps as in https://strimzi.io/docs/operators/latest/quickstart.html to set up the Strimzi operators, the Kafka cluster, zookeeper (use the YAML files available in `/install/cluster-operator`). **Bewere**: you must deploy the extended version of the cluster controller, available now on GitHub (https://github.com/BorisRado/strimzi-kafka-operator) and DockerHub (borisrado/operator:latest);
3. Create the ConfigMap that contains the JSON configuration. For convenience, the JSON configuration should be the only entry of the ConfigMap. For example you can write the JSON configuration in the `rl_example.json` file and later on create the ConfigMap with the command `kubectl create configmap myRlConfigMap --from-file rl_example.json -n kafka`. Of course, the ConfigMap must be in the same namespace as the bridge pods;
4. Deploy the Bridge with two additions: use the `borisrado/kafka-bridge` as the image (`.spec.image`), and set the `.spec.rlConfigMap` field to contain the name of the ConfigMap that contains the rate limiting configuration. In the case you created the ConfigMap with the command described in point 3, you should write `rlConfigMap: myRlConfigMap`;
5. Rate limiting should be now in place.

When you update the configuration file or change the name of the `.spec.rlConfigMap` field, the cluster controller will take over and perform a rolling release, so that the existing pods will be shut down and a set of new ones with the new configuration will be launched.

### 2.4 Possible responses
* `429: TOO MANY REQUESTS`: is returned when the client has reached the maximum number of requests. The response contains the `X-Millis-Until-Refill` header, that informs the client about the time, expressed in milliseconds, it needs to wait until the limit is renowned;
* `403: FORBIDDEN`: is returned when the number of messages inside of the same HTTP requests exceeds the maximum number of allowed messages, which happens for example when a client tries to write a batch of 10 messages to the `foo` topic, but it is allowed to write only 5 requests per second to this topic;
* All the other responses returned when issuing requests to the `/topics/{foo}` and `/topics` route. Note only responses are going to contain the headers as explained in section 2.1.

### Limitations
Current limitations:
* The Hazelcast cluster needs some time to spin up;
* Limited to token bucker algorithm.
