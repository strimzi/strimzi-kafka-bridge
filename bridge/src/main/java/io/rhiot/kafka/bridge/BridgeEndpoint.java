/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rhiot.kafka.bridge;

import io.vertx.core.Handler;
import io.vertx.proton.ProtonLink;

/**
 * Interface for classes which acts as endpoints
 * bridging traffic between AMQP and Apache Kafka
 * 
 * @author ppatierno
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
	 * Handler for the Proton link 
	 * @param link		Proton link to handle
	 */
	void handle(ProtonLink<?> link);
	
	/**
	 * Sets an handler for when an endpoint is closed due to internal processing
	 * 
	 * @param endpoint		The handler
	 * @return				The endpoint
	 */
	BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler);
}
