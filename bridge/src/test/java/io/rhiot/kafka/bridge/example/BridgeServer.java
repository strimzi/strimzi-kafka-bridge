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
package io.rhiot.kafka.bridge.example;

import java.io.IOException;

import io.rhiot.kafka.bridge.Bridge;
import io.rhiot.kafka.bridge.BridgeConfigProperties;
import io.vertx.core.Vertx;

/**
 * Class example on running the bridge server
 * 
 * @author ppatierno
 */
public class BridgeServer {
	
	public static void main(String[] args) {
		
		String config = (args.length > 0 && !args[0].isEmpty()) ? args[0] : null;
		
		Vertx vertx = Vertx.vertx();

		BridgeConfigProperties bridgeConfigProperties = new BridgeConfigProperties();
		
		Bridge bridge = new Bridge(vertx, bridgeConfigProperties);
		bridge.start();
		
		try {
			System.in.read();
			bridge.stop();
			vertx.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
