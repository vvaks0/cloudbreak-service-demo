package com.hortonworks.demostore.model;

import java.util.HashMap;
import java.util.Map;

public class Tree {

	private Core core;
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	public Core getCore() {
	return core;
	}

	public void setCore(Core core) {
	this.core = core;
	}

	public Map<String, Object> getAdditionalProperties() {
	return this.additionalProperties;
	}

	public void setAdditionalProperty(String name, Object value) {
	this.additionalProperties.put(name, value);
	}

}
	