package io.ppatierno.kafka.bridge.example;

import java.io.IOException;

import io.ppatierno.kafka.bridge.Bridge;
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
		
		Bridge bridge = new Bridge(vertx, config);
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
