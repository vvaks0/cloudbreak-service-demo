package com.hortonworks.demostore.model;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Core {

	private List<String> data = null;
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();
	
	public List<String> getData() {
	return data;
	}
	
	public void setData(List<String> data) {
	this.data = data;
	}
	
	public Map<String, Object> getAdditionalProperties() {
	return this.additionalProperties;
	}
	
	public void setAdditionalProperty(String name, Object value) {
	this.additionalProperties.put(name, value);
	}

}