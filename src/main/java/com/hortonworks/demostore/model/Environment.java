package com.hortonworks.demostore.model;

import java.util.Map;

public abstract class Environment {
	public static String apiHost = "localhost";
	public static String apiPort = "21000";
	
	public static void init() {
		Map<String, String> env = System.getenv();
        System.out.println("********************** ENV: " + env);
        if(env.get("NIFI_HOST") != null){
        	apiHost = (String)env.get("NIFI_HOST");
        }
        if(env.get("NIFI_PORT") != null){
        	apiPort = (String)env.get("NIFI_PORT");
        }
		
	}
}
