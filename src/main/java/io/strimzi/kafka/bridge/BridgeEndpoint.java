/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge;

import io.vertx.core.Handler;

/**
 * Interface for classes which acts as endpoints
 * bridging traffic between a protocol and Apache Kafka
 */
public interface BridgeEndpoint {

	/**
	 * Open the bridge endpoint
	 */
	void open();
	
	/**
	 * Close the bridge endpoint
	 */
	void close();
	
	/**
	 * Handler for the remote protocol endpoint
	 * @param endpoint	Remote protocol endpoint to handle
	 */
	void handle(Endpoint<?> endpoint);
	
	/**
	 * Sets an handler called when a bridge endpoint is closed due to internal processing
	 * 
	 * @param endpointCloseHandler	The handler
	 * @return	The bridge endpoint
	 */
	BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler);
}
