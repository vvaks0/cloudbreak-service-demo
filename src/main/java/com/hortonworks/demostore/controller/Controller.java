package com.hortonworks.demostore.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.Charset;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.hortonworks.demostore.model.Atlas;

@Component
@RestController
public class Controller{
	static final Logger LOG = LoggerFactory.getLogger(Controller.class);
	
	@Value("${cloudbreak.api.host}")
	private String cloudbreakApiHost;
	
	@Value("${cloudbreak.api.port}")
	private String cloudbreakApiPort;
	
	private String atlasApiHost;
	private String atlasApiPort;
	private String atlasUrl;
	private String dpsHost;
	private String dpsAdminUserName;
	private String dpsAdminPassword;
	private String sharedServicesClusterId;
	private String sharedServicesClusterName;
	private String sharedServicesAmbariHost;
	private String sharedServicesLdap;
	private String sharedServicesHiveMetastoreRds;
	private String sharedServicesRangerMetastoreRds;
	private String cloudbreakAuthHost;
	private String cloudbreakAuthPort;
	private String cloudbreakUrl;
	private String cloudbreakAuthUrl;
	private String adminUserName;
	private String adminPassword;
	private String cloudbreakApiUri = "/cb/api/v1";
	private String cloudbreakApiUriV2 = "/cb/api/v2";
	private String credentialsUri = "/credentials/account";
	private String blueprintsUri = "/blueprints/account";
	private String templatesUri = "/templates/account";
	private String recipesUri = "/recipes/account";
	private String securityGroupsUri = "/securitygroups/account";
	private String networksUri = "/networks/account";
	private String stacksUri = "/stacks/account";
	private String ldapUri = "/ldap/account";
	private String rdsUri = "/rdsconfigs/account";
	
	private String dpsUrl = "";
	private String dps_auth_uri = "/auth/in";
	private String dps_clusters_uri = "/api/actions/clusters?type=all";
	private String dlm_clusters_uri = "/dlm/api/clusters";
	private String dlm_policies_uri = "/dlm/api/policies?numResults=200&instanceCount=10";
	private String dss_collections_uri = "/api/dataset/list/tag/ALL?offset=0&size=10";
	private String dss_dataset_uri = "/dss/api/dataset"; //2/assets?queryName=&offset=0&limit=20";
	private String dss_assets_uri = "/dss/api/assets/details";
	
	private String ldapSuffix = "-ldap";
	private String hiveRdsSuffix = "-hive-metastore";
	private String rangerRdsSuffix = "-ranger-metastore";
	
	private String ldapPort = "33389";
	private String hiveMetastoreRdsPprt = "5432";
	private String rangerMetastoreRdsPprt = "5432";
	
	private String publicKey = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC4YhcNwxMvZLyLECWfJyqf5rdk+R+DM2gzt0cEFYu9/SVV3GWAGvESevGCMEZaqapMDWgY+9n5uFgQRKo8UeVH1cJuRqQUZOY44ZiZokiMZ+kkY5rPOj44ArKXvqQDz0DB1EVyMYB8LATjloDtcghl51Z/y2hXMjaxpewYokTh8YTeMPvyBYvvIuRIW0EOMMDLXfR3EXaLwQAtlDjWkQYezFnkNM4lQsYJ50ohb/DA68ZCBhvTYqPYPbFmeNHubt/ymucecDaAJFbwmdHf6j+8xbT/HH/4GbCUXgU9RNCKgJfBlOcgEEBCy0cbg/hFz2sawMA+epuX4OhY2s9atugV cloudbreak";

	private String oAuthToken = null;
	
	private int storedHour = 0;
	private int storedDay = 0;
	private int storedYear = 0;
	
	Map<String, HashMap<String, Object>> platformMetaData = new HashMap<String, HashMap<String,Object>>();

	public Controller() {
		Map<String, String> env = System.getenv();
		
		if(env.get("ADMIN_USER_NAME") != null){
			adminUserName=(String)env.get("ADMIN_USER_NAME");
		}
		if(env.get("ADMIN_PASSWORD") != null){
			adminPassword = (String)env.get("ADMIN_PASSWORD");
		}		
		if(env.get("API_HOST") != null){
			cloudbreakApiHost = (String)env.get("API_HOST");
        }
        if(env.get("API_PORT") != null){
        		cloudbreakApiPort = (String)env.get("API_PORT");
        }
        if(env.get("AUTH_HOST") != null){
        		cloudbreakAuthHost = (String)env.get("AUTH_HOST");
        }
        if(env.get("AUTH_PORT") != null){
        		cloudbreakAuthPort = (String)env.get("AUTH_PORT");
        }
        if(env.get("DPS_HOST") != null){
    			dpsHost = (String)env.get("DPS_HOST");
    			dpsUrl = "https://"+dpsHost;
        }
        if(env.get("DPS_ADMIN_USER_NAME") != null){
        		dpsAdminUserName=(String)env.get("DPS_ADMIN_USER_NAME");
        }
        if(env.get("DPS_ADMIN_PASSWORD") != null){
        		dpsAdminPassword = (String)env.get("DPS_ADMIN_PASSWORD");
        }
        /*
        if(env.get("ATLAS_HOST") != null){
        		atlasApiHost = (String)env.get("ATLAS_HOST");
        }
        if(env.get("ATLAS_PORT") != null){
        		atlasApiPort = (String)env.get("ATLAS_PORT");
        }
        if(env.get("SHARED_SERVICES_AMBARI_HOST") != null){
        		sharedServicesAmbriHost = (String)env.get("SHARED_SERVICES_AMBARI_HOST");
        }*/
        
		initializeTrustManager();
		
		try {
			if(dpsUrl != null) {
				getSharedServicesService();
			}else {
				LOG.error("********** Shared Services Host is not configured, please set environment variable DPS_HOST...");
				System.exit(1);
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
        atlasUrl = "http://"+atlasApiHost+":"+atlasApiPort;
        cloudbreakUrl = "https://"+cloudbreakApiHost+":"+cloudbreakApiPort;
        cloudbreakAuthUrl = "http://"+cloudbreakAuthHost+":"+cloudbreakAuthPort+"/oauth/authorize?response_type=token&client_id=cloudbreak_shell";
        System.out.println("********************** Controller ()  DPS Url : " + dpsUrl);
        System.out.println("********************** Controller ()  Shared Services Amabri : " + sharedServicesAmbariHost);
        System.out.println("********************** Controller ()  Cloudbreak API url set : " + cloudbreakUrl);
        System.out.println("********************** Controller ()  Cloudbreak Auth url set : " + cloudbreakAuthUrl);
        
		int currentHour = getCurrentHour();
		int currentDay = getCurrentDay();
		int currentYear = getCurrentYear();
		
		System.out.println(currentHour+":"+storedHour);
		if(currentYear >= storedYear && currentDay >= storedDay && currentHour > storedHour){
			String credentials = "credentials={\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
			LOG.info("********************** Controller ()  Credentials : " +credentials);
			oAuthToken = getToken(cloudbreakAuthUrl, credentials);
			//System.out.println("********** Auth Token: "+oAuthToken);
			LOG.debug("********************** Controller ()  Cloudbreak API OAuth Token set : " + oAuthToken);
			storedHour = currentHour;
		}
        
		
		/*
        String[] platforms = {"AWS","OPENSTACK"};
        
        for (String platform : platforms){
        	HashMap<String,Object> platformComponents = new HashMap<String, Object>();
        	platformComponents.put("credentials", getCredentials(platform));
        	platformComponents.put("templates", getTemplates(platform));
        	platformComponents.put("securityGroups", getSecurityGroups(platform));
        	platformComponents.put("networks", getNetworks(platform));
        	platformComponents.put("blueprints", getBlueprints());
        	platformComponents.put("recipes", getRecipes());
        	platformMetaData.put(platform, platformComponents);
        }*/
        getAllArtifacts();
        System.out.println("********************** Controller ()  Platform Metadata has been loaded: " + platformMetaData);
	}
	
	@RequestMapping(value="/getPlatformComponents", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object> getPlatformComponents(@RequestParam(value="platform") String platform) {
		System.out.println(platformMetaData.get(platform));
    		return platformMetaData.get(platform);
    }
	
	@RequestMapping(value="/refreshArtifacts", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public void getAllArtifacts() {
		 String[] platforms = {"AWS","OPENSTACK","GCP","AZURE"};
		 HashMap<String,Object> platformComponents = new HashMap<String, Object>();
		 
		 for (String platform : platforms){
			platformComponents = new HashMap<String, Object>();
			platformComponents.put("credentials", getCredentials(platform));
	        platformComponents.put("templates", getTemplates(platform));
	        //platformComponents.put("securityGroups", getSecurityGroups(platform));
	        //platformComponents.put("networks", getNetworks(platform));
	        platformMetaData.put(platform, platformComponents);
		 }
		 
		 //platformComponents.put("blueprints", getBlueprints());
	     platformComponents.put("recipes", getRecipes());
    }
	
	@RequestMapping(value="/refreshAllClusters", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    private HashMap<String, Object> getClusterStatus() {
		int currentHour = getCurrentHour();
		int currentDay = getCurrentDay();
		int currentYear = getCurrentYear();
		
		System.out.println(currentHour+":"+storedHour);
		if(currentYear >= storedYear && currentDay >= storedDay && currentHour > storedHour){
			String credentials = "credentials={\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
			oAuthToken = getToken(cloudbreakAuthUrl, credentials);
			//System.out.println("********** Auth Token: "+oAuthToken);
			LOG.debug("********************** Controller ()  Cloudbreak API OAuth Token set : " + oAuthToken);
			storedHour = currentHour;
		}
		
		String urlString = cloudbreakUrl+cloudbreakApiUri+stacksUri;
	    HashMap<String, Object> clusters = new HashMap<String,Object>();
	    JSONArray clustersJSON = httpGetArray(urlString);
	    System.out.println("********** Cluster: " + clustersJSON);
	    try {
			getSharedServicesService();
			
			if(sharedServicesAmbariHost != null && sharedServicesLdap == null) {
				JSONObject ldapResult = httpGetObject(cloudbreakUrl+cloudbreakApiUri+ldapUri+"/"+sharedServicesClusterName+ldapSuffix);
				LOG.info("********** Checking for Shared Services Ldap: " + ldapResult);
				if(ldapResult.isNull("name")) {
					createSharedServicesLdap(sharedServicesClusterName, sharedServicesAmbariHost, ldapPort);
				}
				sharedServicesLdap = sharedServicesClusterName+ldapSuffix;
			}
			if(sharedServicesAmbariHost != null && sharedServicesHiveMetastoreRds == null) {
				JSONObject hiveMetastoreResult = httpGetObject(cloudbreakUrl+cloudbreakApiUri+rdsUri+"/"+sharedServicesClusterName+hiveRdsSuffix);
				LOG.info("********** Checking for Shared Services Hive Metastore: " + hiveMetastoreResult);
				if(hiveMetastoreResult.isNull("name")) {				
					createSharedServicesHiveMetastoreRds(sharedServicesClusterName, sharedServicesAmbariHost, hiveMetastoreRdsPprt);
				}
				sharedServicesHiveMetastoreRds = sharedServicesClusterName+hiveRdsSuffix;
			}
			if(sharedServicesAmbariHost != null && sharedServicesRangerMetastoreRds == null) {
				JSONObject rangerMetastoreResult = httpGetObject(cloudbreakUrl+cloudbreakApiUri+rdsUri+"/"+sharedServicesClusterName+rangerRdsSuffix);
				LOG.info("********** Checking for Shared Services Ranger Metastore: " + rangerMetastoreResult);
				if(rangerMetastoreResult.isNull("name")) {				
					createSharedServicesRangerMetastoreRds(sharedServicesClusterName, sharedServicesAmbariHost, rangerMetastoreRdsPprt);
				}
				sharedServicesRangerMetastoreRds = sharedServicesClusterName+rangerRdsSuffix;
			}
		} catch (JsonParseException e1) {
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (JSONException e1) {
			e1.printStackTrace();
		}
	    for(int i=0;i<clustersJSON.length();i++){
	    		HashMap<String, Object> cluster = new HashMap<String,Object>();
	    		String clusterType = null;
	    		boolean isSharedServices = false;
	    		try {
	    			String blueprintName = clustersJSON.getJSONObject(i).getJSONObject("cluster").getJSONObject("blueprint").getString("name");
	    			if(blueprintName != null){
	    				if(blueprintName.split("_v")[0].equalsIgnoreCase("hdp-hdf-multi-node") || (blueprintName.split("_V")[0].equalsIgnoreCase("hdp-hdf-multi-node"))){
	    					clusterType = "Connected Platform";
	    				}else if(blueprintName.split("_v")[0].equalsIgnoreCase("historian-multi-node") || blueprintName.split("_v")[0].equalsIgnoreCase("historian-multi-node-scalable")){
	    					clusterType = "Historian";
	    				}else if (blueprintName.split("_v")[0].equalsIgnoreCase("shared-services") || (blueprintName.split("-V")[0].equalsIgnoreCase("shared-services"))){
	    					clusterType = "Shared Services";
	    					isSharedServices = true;
	    				}else if (blueprintName.split("_v")[0].equalsIgnoreCase("dps-managed") || (blueprintName.split("-V")[0].equalsIgnoreCase("dps-managed"))){
	    					clusterType = "DPS Managed";  		
	    				}else if (blueprintName.split("_v")[0].equalsIgnoreCase("temp-workspace") || (blueprintName.split("-V")[0].equalsIgnoreCase("temp-workspace"))){
	    					clusterType = "Semi-Ephemeral";  		
	    				}else if (blueprintName.split("_v")[0].equalsIgnoreCase("ephemeral") || (blueprintName.split("-V")[0].equalsIgnoreCase("ephemeral"))){
	    					clusterType = "Ephemeral";  		
	    				}
	    			}
	    			if(clusterType != null){
	    				String clusterId = clustersJSON.getJSONObject(i).getString("id");
	    				cluster.put("clusterId", clusterId);
	    			
	    				String clusterName = clustersJSON.getJSONObject(i).getString("name");
	    				cluster.put("clusterName", clusterName);
	    			
	    				String platform = clustersJSON.getJSONObject(i).getString("cloudPlatform");
	    				cluster.put("platform", platform);
	    			
	    				cluster.put("clusterType", clusterType);
	    				cluster.put("templateName",clustersJSON.getJSONObject(i).getJSONArray("instanceGroups").getJSONObject(0).getJSONObject("template").getString("instanceType"));
	    			
	    				String ambariServerIp = clustersJSON.getJSONObject(i).getJSONObject("cluster").getString("ambariServerIp");
	    				ambariServerIp = (ambariServerIp == null) ? "PENDING" : ambariServerIp;
	    				cluster.put("clusterAmbariIp", ambariServerIp);
	    			
	    				String clusterStatus = clustersJSON.getJSONObject(i).getString("status");
	    				if(clusterStatus.equalsIgnoreCase("STOPPED") || clusterStatus.equalsIgnoreCase("DELETE_IN_PROGRESS")){
	    				clusterStatus = clustersJSON.getJSONObject(i).getString("status");
	    				}else if(ambariServerIp.equalsIgnoreCase("PENDING")){
	    					clusterStatus = clustersJSON.getJSONObject(i).getString("statusReason");
	    				}else{ 
	    					clusterStatus = getLastAmbariTask(ambariServerIp, clusterName);
	    				}
	    				cluster.put("clusterStatus", clusterStatus);
	    				cluster.put("isSharedServices", isSharedServices);
	    				clusters.put(clusterId, cluster);
	    			}
	    		} catch (JSONException e) {
	    			e.printStackTrace();
	    		} 
	    }	
	    return clusters;
    }
	
	@RequestMapping(value="/terminateCluster", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object> terminateCluster(@RequestParam(value="clusterId") String clusterId) {
		HashMap<String, Object> deleteClusterResponse = new HashMap<String, Object>();
		String urlString = cloudbreakUrl+cloudbreakApiUri+"/stacks/"+clusterId;
		System.out.println("********** Sending Cluster Termination request to: "+ urlString);
		deleteClusterResponse.put("responseCode", httpDeleteObject(urlString));
		System.out.println("********* "+deleteClusterResponse);
    	return deleteClusterResponse;
    }

	private String getLastAmbariTask(String ambariIp, String clusterName){
		String urlString = "http://"+ambariIp+":8080/api/v1/clusters/"+clusterName+"/requests";
		String basicAuth = "Basic " + new String(Base64.encodeBase64("admin:admin".getBytes()));
		String currentTask = null;
		int itemsLength;
		try {
			JSONObject requestsJSON = httpGetObject(urlString, basicAuth, false);
			if(requestsJSON==null){
				return "Ambari conncetion timed out...";
			}	
			itemsLength = requestsJSON.getJSONArray("items").length();
			urlString = requestsJSON.getJSONArray("items").getJSONObject(itemsLength-1).getString("href");
			JSONObject requestJSON = httpGetObject(urlString, basicAuth, false);
			currentTask = requestJSON.getJSONObject("Requests").getString("request_context");
		} catch (JSONException e) {
			LOG.error("Connection timed out: " + urlString);
		}
		
		return currentTask;
	}
	
    @RequestMapping(value="/search", produces = { MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<String>> search(@RequestParam(value="term") String text) {
    	
    	List<String> searchResults = new ArrayList<String>();

    	Lists.newArrayList("cat","mouse","dog");
    	LOG.trace(text);

    	for (String name: Lists.newArrayList("cat","mouse","dog") ) {
    		if (name.toLowerCase().contains(text.toLowerCase())) {
	    		LOG.debug("match: "+ name);
	    		searchResults.add(name);
    		}
    	}
    	
    	if (!searchResults.isEmpty()) {
    		return new ResponseEntity<List<String>>(searchResults, HttpStatus.OK);
    	}
    	else {
    		return new ResponseEntity<List<String>>(searchResults, HttpStatus.BAD_REQUEST);
    	}	
    }
	
    @RequestMapping(value="/getfiletree", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object>  getFileTree() {
    
    		return Atlas.atlasFileTree;
    	/*
    	
    	HashMap<String, Object> coreHm = new HashMap<String, Object>();
    	HashMap<String, Object> dataHm = new HashMap<String, Object>();
    	List<Object> data = new ArrayList<Object>();
    	//data.add("Empty Folder");    	
       	
    	RestTemplate restTemplate = new RestTemplate();
    	
    	restTemplate.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

    	data = callAtlas("http://xxx:21000/api/atlas/v1/taxonomies/", restTemplate, data);
    	
    	dataHm.put("data", data);
    	coreHm.put("core", dataHm);   	
    	    	
    	return coreHm;
    	*/
    }
    
    @RequestMapping(value="/refreshCatalog", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public void refreshAtlas() {
    		Atlas.init();
    }
    
    @SuppressWarnings("unchecked")
   	@RequestMapping(value="/getDssEntity", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    private HashMap<String, Object> getDssEntity(
    	@RequestParam(value="collectionId") String collectionId, 
    	@RequestParam(value="assetGuid") String assetGuid) {
       	HashMap<String, Object> entities = new HashMap<String, Object>();
       	ObjectMapper mapper = new ObjectMapper();
       	
     	String token = getDpsToken(dpsUrl+dps_auth_uri).get(0);
     	String assetClusterId = null; 
     	
     	LOG.info("+++++++++++++ " + dpsUrl+dss_dataset_uri+"/"+collectionId+"/assets?queryName=&offset=0&limit=20");
     	try {
     		JSONArray assets = httpGetDpsObject(dpsUrl+dss_dataset_uri+"/"+collectionId+"/assets?queryName=&offset=0&limit=20", token).getJSONArray("assets");
     	
     		for(int i=0; i < assets.length(); i++) { 
				if(assets.getJSONObject(i).getString("guid").equalsIgnoreCase(assetGuid)) {
					assetClusterId = assets.getJSONObject(i).getString("clusterId");
				}
     		}
     	
     		JSONObject asset = httpGetDpsObject(dpsUrl+dss_assets_uri+"/"+assetClusterId+"/"+assetGuid, token);
     		LOG.info("+++++++++++++ " + asset);
		
     		Iterator assetKeys = asset.keys();
     		//while(assetKeys.hasNext()) {
			
     		//}
     		entities = mapper.readValue(asset.toString(), HashMap.class);
     	} catch (JSONException e) {
     		e.printStackTrace();
     	} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
     	
       	return entities;
    }
    
    private JSONObject httpGetDpsObject(String urlString, String token) {
	    JSONObject response = null;
	    //String userpass = adminUserName + ":" + adminPassword;
	    //String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
		try {
			URL url = new URL (urlString);
			HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);            
			//connection.setRequestProperty ("Authorization", basicAuth);
			connection.setRequestProperty  ("Cookie", token);
			InputStream content = (InputStream)connection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
	      	String jsonText = readAll(rd);
	      	response = new JSONObject(jsonText);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return response;
    }
    
    private JSONArray httpGetDpsArray(String urlString, String token) {
	    JSONArray response = null;
	    //String userpass = adminUserName + ":" + adminPassword;
	    //String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
		try {
			URL url = new URL (urlString);
			HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);            
			//connection.setRequestProperty ("Authorization", basicAuth);
			connection.setRequestProperty  ("Cookie", token);
			InputStream content = (InputStream)connection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
	      	String jsonText = readAll(rd);
	      	response = new JSONArray(jsonText);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return response;
    }
    
    @SuppressWarnings("unchecked")
	@RequestMapping(value="/getAtlasEntity", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object>  getAtlasEntity(@RequestParam(value="entityName") String entityName) {
    	HashMap<String, Object> entities = new HashMap<String, Object>();
    	ObjectMapper mapper = new ObjectMapper();
    	
    	String[] url = {atlasUrl};
		String[] auth = {"admin","admin"};
        
		try {
			AtlasClient atlasClient2 = new AtlasClient(url,auth);
			
			System.out.println(atlasClient2.getAdminStatus());
			//System.out.println(atlasClient2.getEntity("49927a0a-22e2-41b9-af34-6d92099799d2"));
			//JSONObject entity = new JSONObject(InstanceSerialization._toJson(atlasClient2.getEntity(entityName),true));
			//JSONArray entitiesArray = atlasClient2.searchByDSL("hive_table where qualifiedName = 'warehouse."+entityName.replace("'", "")+"@sharedservices'", -1, 0);
			JSONArray entitiesArray = atlasClient2.searchByDSL("hive_table where qualifiedName = "+entityName, -1, 0);
			System.out.println(entitiesArray);
			//entities = mapper.readValue(entity.toString(), HashMap.class);
			entities = mapper.readValue(entitiesArray.get(0).toString(), HashMap.class);
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
    	return entities;
    }
    
    private HashMap<String, Object> getBlueprints() {
    	HashMap<String, Object> blueprints = new HashMap<String, Object>();
    	String urlString = cloudbreakUrl+cloudbreakApiUri+blueprintsUri;
    	double hdp_hdf_version=0;
    	double historian_version=0;
    	double shared_services_version=0;
    	String hdp_hdf_name="none";
    	String historian_name="none";
    	String shared_services_name="none";
    	
    	JSONArray blueprintsJSON = httpGetArray(urlString);
    	System.out.println(blueprintsJSON);
    	for(int i=0;i<blueprintsJSON.length();i++){
    		try {
			JSONObject blueprint = blueprintsJSON.getJSONObject(i); 
			String[] blueprint_name = new String[2];
			System.out.println(blueprint.getString("name"));
			if(blueprint.getString("name").contains("_v")) {
				 blueprint_name = blueprint.getString("name").split("_v");
			}else if(blueprint.getString("name").contains("-v")) {
				blueprint_name = blueprint.getString("name").split("-v");
			}else if(blueprint.getString("name").contains("-V")) {
				blueprint_name = blueprint.getString("name").split("-V");
			}else if(blueprint.getString("name").contains("_V")) {
				blueprint_name = blueprint.getString("name").split("_V");
			}else {
				blueprint_name[0] = blueprint.getString("name");
			}
    			System.out.println("**********");
    			System.out.println(blueprint.getString("name"));
    			System.out.println("**********");
    			if(blueprint_name[0].equalsIgnoreCase("hdp-hdf-multi-node") && blueprint_name.length > 1){
    				if (hdp_hdf_version==0 || hdp_hdf_version < Double.valueOf(blueprint_name[1])){
    			        hdp_hdf_version = Double.valueOf(blueprint_name[1]);
    			        hdp_hdf_name = blueprint.getString("name");
    			        System.out.println(hdp_hdf_name);
    			        blueprints.put(blueprint_name[0],blueprint.getInt("id"));
    			        //blueprints.put(hdp_hdf_name,blueprint.getInt("id"));
    			      }
    			}else if(blueprint_name[0].equalsIgnoreCase("historian-multi-node") && blueprint_name.length > 1){
    				if (historian_version==0 || historian_version < Double.valueOf(blueprint_name[1])){
    					historian_version = Double.valueOf(blueprint_name[1]);
  			        	historian_name = blueprint.getString("name");
  			        	System.out.println(historian_name);
  			        	blueprints.put(blueprint_name[0],blueprint.getInt("id"));
  			        	//blueprints.put(historian_name,blueprint.getInt("id"));
    				}
    			}else if (blueprint_name[0].equalsIgnoreCase("shared-services") && blueprint_name.length > 1){
    				if (shared_services_version==0  || shared_services_version < Double.valueOf(blueprint_name[1])){
    			        shared_services_version = Double.valueOf(blueprint_name[1]);
    			        shared_services_name = blueprint.getString("name");
    			        System.out.println(shared_services_name);
  			        	blueprints.put(blueprint.getString("name"),blueprint.getInt("id"));
    			        //blueprints.put(shared_services_name,blueprint.getInt("id"));
    				}    		
    			}
			} catch (JSONException e) {
				e.printStackTrace();
			}
    	}
    	
    System.out.println(blueprints);
    	return blueprints;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
	@RequestMapping(value="/createCluster", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    private HashMap<String,Object> createCluster(
    		@RequestParam(value="clusterName") String clusterName, 
    		@RequestParam(value="clusterType") String type, 
    		@RequestParam(value="platform") String platform, 
    		@RequestParam(value="templateId") String templateId, 
    		@RequestParam(value="credentialId") String credentialId,
    		@RequestParam(value="sharedServicesIp") String sharedServicesIp,
    		@RequestParam(value="targetBucket") String targetBucket,
    		@RequestParam(value="sourceClusterId") String sourceClusterId,
    		@RequestParam(value="sourceDatasetName") String sourceDatasetName) {
    	
    		HashMap<String, Object> responseMap = null;
    		String urlString = cloudbreakUrl+cloudbreakApiUri+stacksUri;
    	
    		String region = null;
		String zone = null;
		String variant = null;
		String stackOs = null;
		String securityGroupId = null;
		String vpcId = null;
		String networkId = null;
		String subnetId = null;
		String blueprintId = null;
		String blueprint = null;
		String recipeId = null;
		String instanceType = null;
		String volumeType = null;
		String volumeCount = null;
		String imageCatalog = "cloudbreak-default";
		String imageId = null;
		String ambariRepoBaseUrl = null;
		String stackDefUrl = null;
		String s3ArnRole = "";
		String workerCount = "1";
    		//String ambariRepoVersion = "2.6.1.3";
		String ambariRepoVersion = "2.6.2.0";
    		String ambariRepoGpgKey = "http://public-repo-1.hortonworks.com/ambari/centos7/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins";
    		String hdpPlatformVerson = "HDP 2.6";
    		String stackMajorVersion = "2.6";
    		//String stackRepoVersion = "2.6.4.5-2";
    		String stackRepoVersion = "2.6.5.0-292";
    		if(platform.equalsIgnoreCase("AWS")) {
    			//ambariRepoBaseUrl = "http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.1.3";
        		ambariRepoBaseUrl = "http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.6.2.0";
        		//stackDefUrl = "http://private-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.4.5-2/HDP-2.6.4.5-2.xml";
        		stackDefUrl = "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.6.5.0/HDP-2.6.5.0-292.xml";
        		s3ArnRole = "arn:aws:iam::081339556850:instance-profile/shared-services-s3-access";
    		}else {
    			//ambariRepoBaseUrl = "http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.1.3";
    			ambariRepoBaseUrl = "http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.2.0";
    			//stackDefUrl = "http://private-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.4.5-2/HDP-2.6.4.5-2.xml";
    			stackDefUrl = "http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/HDP-2.6.5.0-292.xml";
    		}
    		String mpacks = "{\"name\":\"dlm-beacon-centos7-1-1-0\"},{\"name\":\"dss-dpprofiler-centos7-1-0-0\"}";
    		String recipes = "";
    		String rdsConfigs = "";
    		String ldapConfigName = "null";
    		String sharedServices = "";
    		
		//credentialId = (String) ((HashMap)platformMetaData.get(platform).get("credentials")).get(credential);
		//templateId = (String) ((HashMap)platformMetaData.get(platform).get("templates")).get(template);
    		System.out.println(platform);	
    		if (platform.equalsIgnoreCase("AWS")){
    		  region="us-east-1";
    		  zone="null";
    		  variant="AWS";
    		  stackOs = "centos6";
    		  //securityGroupId = (String) ((HashMap)platformMetaData.get(platform).get("securityGroups")).get("aws-connected-platform-all-services-v1");
    		  securityGroupId = "sg-6bb0ac22";
    		  //networkId = (String) ((HashMap)platformMetaData.get(platform).get("networks")).get("aws-existing-vpc-subnet");
    		  vpcId = "vpc-d85076bd";
    		  networkId = null;
    		  subnetId = "subnet-dff56386";
    		  //imageId = "63cdb3bc-28a6-4cea-67e4-9842fdeeaefb"; //HDP 2.6.4.5 image
    		  imageId = "0f575e42-9d90-4f85-5f8a-bdced2221dc3"; //Base Image
    		  instanceType = "m3.xlarge";
    		  volumeType = "standard";
    		  volumeCount = "1";
    		  recipes = "\"configure-postgres-metastores\",\"install-dps-agents-1-1-0\",\"dps-dlm-remove-cluster-aws-1-1-0\"";
    		  mpacks = "{\"name\":\"dlm-beacon-centos6-1-1-0\"},{\"name\":\"dss-dpprofiler-centos6-1-0-0\"}";
    		}else if (platform.equalsIgnoreCase("OPENSTACK")){
    		  region="RegionOne";
    		  zone="SE";
    		  variant="HEAT";
    		  stackOs = "centos7";
    		  securityGroupId = "de632b11-944c-4acb-a0cd-47f864150d5e";
    		  vpcId = null;
    		  networkId = "71a870bb-191c-4abe-bf02-ece2e9b3345c";
    		  subnetId = "aa7c8bb9-0152-46b9-8596-935baca704a0";
    		  //imageId = "74d99079-f5d0-4470-78b3-11ceaf944069"; //HDP 2.6.4.5 image
    		  imageId = "083f7289-876f-4267-436b-ece5504adb06"; //Base Image
    		  instanceType = "m3.xlarege";
    		  volumeType = "HDD";
    		  volumeCount = "0";
    		  recipes = "\"configure-postgres-metastores\",\"install-dps-agents-1-1-0\",\"dps-dlm-remove-cluster-openstack-1-1-0\"";
    		}else if (platform.equalsIgnoreCase("GCP")){
  		  region="us-east1";
  		  zone="us-east1-b";
  		  variant="GCP";
  		  stackOs = "centos7";
  		  securityGroupId = "a-ephemeral-master";
  		  vpcId = null;
  		  networkId = "default";
  		  subnetId = "default";
  		  //imageId = "5a25167d-656a-4621-4755-0e8e6381e4fe"; //HDP 2.6.4.5 image
  		  imageId = "4102fae2-5de9-4bbd-4c6c-de663c87f6d0"; //Base Image
  		  instanceType = "n1-standard-4";
  		  volumeType = "pd-standard";
  		  volumeCount = "1";
  		  recipes = "\"configure-postgres-metastores\",\"install-dps-agents-1-1-0\",\"dps-dlm-remove-cluster-gcp-1-1-0\"";  
    		}else{
    			System.out.println("********************Invalid cloud platform requested...");
    			System.out.println("********************Valid platform types are: AWS, OPENSTACK, GCP");
    			return null;
    		}
    	/*
    	if (type.equalsIgnoreCase("connected-platform")){
    		blueprintId = String.valueOf(((HashMap)platformMetaData.get(platform).get("blueprints")).get("hdp-hdf-multi-node"));
    		recipeId = String.valueOf(((HashMap)platformMetaData.get(platform).get("recipes")).get("canonical-hdf-service-install"));
    	}else if (type.equalsIgnoreCase("historian")){    		  
    		blueprintId = String.valueOf(((HashMap)platformMetaData.get(platform).get("blueprints")).get("historian-multi-node"));
    		recipeId = String.valueOf(((HashMap)platformMetaData.get(platform).get("recipes")).get("historian-install"));
    	}else if (type.equalsIgnoreCase("shared-services")){    		  
    		blueprintId = String.valueOf(((HashMap)platformMetaData.get(platform).get("blueprints")).get("shared-services"));
    		//recipeId = String.valueOf(((HashMap)platformMetaData.get(platform).get("recipes")).get("shared-service-client-post-install"));
    		recipeId = (String) createEphemeralRecipe(clusterName, sharedServicesAmabriHost, targetBucket, "sharedservices_hive").get(clusterName);
	}else{
    		System.out.println("********************Invalid cluster type selected ("+type+")...");
    		return null;
    	}
    	*/
    	
    		String stackDef = null;
    		urlString = cloudbreakUrl+cloudbreakApiUriV2+stacksUri;
    		LOG.info("********** Cluster Type:" + type);
    		if (type.equalsIgnoreCase("semi-ephemeral")){    	
    			blueprint = "TEMP-WORKSPACE-V1.2";
    			recipeId = createSemiEphemeralRecipe(clusterName, sourceClusterId, sourceDatasetName);
    			recipes += ",\""+recipeId+"\"";
    			workerCount = "3";
    		}else if(type.equalsIgnoreCase("ephemeral")) {
    			blueprint = "EPHEMERAL-V1.5";
    			//recipeId = createEphemeralRecipe(clusterName, sharedServicesIp, targetBucket, "sharedservices_hive").toString();
    			//recipes += ",\""+recipeId+"\"";
    			recipes = "";
    			sharedServices = "\"sharedCluster\": \""+sharedServicesClusterName+"\"";
    			rdsConfigs = "\""+sharedServicesHiveMetastoreRds+"\", \""+sharedServicesRangerMetastoreRds+"\"";
    			ldapConfigName = "\""+sharedServicesLdap+"\"";
    			mpacks = "";
    		}else if(type.equalsIgnoreCase("dps-managed")) {
    			blueprint = "DPS-MANAGED-V1.6";
    			workerCount = "3";
    			if(platform.equalsIgnoreCase("GCP")) {
    				recipes += ",\"dps-dlm-register-cluster-gcp-1-1-0\",\"load-logistics-dataset\"";
    			}else if(platform.equalsIgnoreCase("AWS")) {
    				recipes += ",\"dps-dlm-register-cluster-aws-1-1-0\",\"load-logistics-dataset\"";
    			}else if(platform.equalsIgnoreCase("OPENSTACK")) {
    				recipes += ",\"dps-dlm-register-cluster-openstack-1-1-0\",\"load-logistics-dataset\"";
    			}
    		}else if(type.equalsIgnoreCase("shared-services")) {
    			blueprint = "SHARED-SERVICES-V1.18";
    			workerCount = "3";
    			if(platform.equalsIgnoreCase("GCP")) {
    				recipes += ",\"dps-dlm-register-cluster-sharedservices-gcp-1-1-0\"";
    			}else if(platform.equalsIgnoreCase("AWS")) {
    				recipes += ",\"dps-dlm-register-cluster-sharedservices-aws-1-1-0\"";
    			}else if(platform.equalsIgnoreCase("OPENSTACK")) {
    				recipes += ",\"dps-dlm-register-cluster-sharedservices-openstack-1-1-0\"";
    			}	
    		}
    		
    		stackDef = "" +
    				"{\n" + 
    				"  \"general\": {\"credentialName\": \""+credentialId+"\",\"name\": \""+clusterName+"\"},\n" + 
    				"  \"placement\": {\"region\": \""+region+"\",\"availabilityZone\": \""+zone+"\"},\n" + 
    				"  \"tags\": {\"userDefinedTags\": {}},\n" + 
    				"  \"cluster\": {\n" + 
    				"  	 \"sharedService\": {"+sharedServices+"}," +
    				"    \"ambari\": {\n" + 
    				"      \"blueprintName\": \""+blueprint+"\",\n" + 
    				"      \"platformVersion\": \""+hdpPlatformVerson+"\",\n" + 
    				"      \"ambariRepoDetailsJson\": {\n" + 
    				"        \"version\": \""+ambariRepoVersion+"\",\n" + 
    				"        \"baseUrl\": \""+ambariRepoBaseUrl+"\",\n" + 
    				"        \"gpgKeyUrl\": \""+ambariRepoGpgKey+"\"\n" + 
    				"      },\n" + 
    				"      \"ambariStackDetails\": {\n" +  
    				"        \"verify\": false,\n" + 
    				"        \"enableGplRepo\": false,\n" + 
    				"        \"stack\": \"HDP\",\n" + 
    				"        \"version\": \""+stackMajorVersion +"\",\n" +
    				"        \"repositoryVersion\": \""+stackRepoVersion+"\",\n" + 
    				"        \"versionDefinitionFileUrl\": \""+stackDefUrl+"\",\n" + 
    				"        \"stackOs\": \""+stackOs+"\",\n" + 
    				"        \"mpacks\": ["+mpacks+"]\n" + 
    				"      },\n" + 
    				"      \"userName\": \"admin\",\n" + 
    				"      \"password\": \"admin\",\n" + 
    				"      \"gateway\": {\n" + 
    				"        \"enableGateway\": false,\n" + 
    				"        \"gatewayType\": \"INDIVIDUAL\"\n" + 
    				"      },\n" + 
    				"      \"validateBlueprint\": false,\n" + 
    				"      \"ambariSecurityMasterKey\": null\n" + 
    				"    },\n" + 
    				"    \"rdsConfigNames\": ["+rdsConfigs+"],\n" + 
    				"    \"ldapConfigName\": "+ldapConfigName+",\n" + 
    				"    \"proxyName\": null\n" + 
    				"  },\n" + 
    				"  \"flexId\": null,\n" + 
    				"  \"imageSettings\": {\n" + 
    				"    \"imageCatalog\": \""+imageCatalog+"\",\n" + 
    				"    \"imageId\": \""+imageId+"\"\n" + 
    				"  },\n" + 
    				"  \"imageType\": \"base\",\n" + 
    				"  \"instanceGroups\": [\n" + 
    				"    {\"parameters\": {},\n" + 
    				"      \"template\": {\n" + 
    				"        \"parameters\": {\"encrypted\": false},\n" + 
    				"        \"instanceType\": \""+instanceType+"\",\n" + 
    				"        \"volumeType\": \""+volumeType+"\",\n" + 
    				"        \"volumeCount\": "+volumeCount+",\n" + 
    				"        \"volumeSize\": 100\n" + 
    				"      },\n" + 
    				"      \"nodeCount\": 1,\n" + 
    				"      \"group\": \"master\",\n" + 
    				"      \"type\": \"GATEWAY\",\n" + 
    				"      \"recoveryMode\": \"MANUAL\",\n" + 
    				"      \"recipeNames\": ["+recipes+"],\n" + 
    				"      \"securityGroup\": {\"securityGroupId\": \""+securityGroupId+"\"}\n" + 
    				"    },\n" + 
    				"    {\n" + 
    				"      \"parameters\": {},\n" + 
    				"      \"template\": {\n" + 
    				"        \"parameters\": {\"encrypted\": false},\n" +
    				"        \"instanceType\": \""+instanceType+"\",\n" +
    				"        \"volumeType\": \""+volumeType+"\",\n" + 
    				"        \"volumeCount\": "+volumeCount+",\n" + 
    				"        \"volumeSize\": 100\n" + 
    				"      },\n" + 
    				"      \"nodeCount\": "+workerCount+",\n" + 
    				"      \"group\": \"worker\",\n" + 
    				"      \"type\": \"CORE\",\n" + 
    				"      \"recoveryMode\": \"MANUAL\",\n" + 
    				"      \"recipeNames\": [ ],\n" + 
    				"      \"securityGroup\": {\"securityGroupId\": \""+securityGroupId+"\"}\n" + 
    				"    }\n" + 
    				"  ],\n" + 
    				"  \"network\": {\n" + 
    				"      \"parameters\": {\"vpcId\":\""+vpcId+"\",\"networkId\":\""+networkId+"\",\"subnetId\":\""+subnetId+"\"," + 
    				"      \"publicNetId\": null,\n" + 
    				"      \"routerId\": null,\n" + 
    				"      \"internetGatewayId\": null,\n" +
    				"      \"networkingOption\": \"provider\"\n" + 
    				"    },\n" + 
    				"    \"subnetCIDR\": null\n" + 
    				"  },\n" + 
    				"  \"stackAuthentication\": {\"publicKeyId\": \"field\", \"publicKey\": \""+publicKey+"\"},\n" +
    				"  \"parameters\":{\"instanceProfileStrategy\":\"USE_EXISTING\",\"instanceProfile\":\""+s3ArnRole+"\"}\n" +
    				"}";

    		System.out.println("********** Stack Def:" + stackDef + " to " + urlString);
    		JSONObject postStackResponse = httpPostObject(urlString, stackDef); 
    		System.out.println("********** Response:" + postStackResponse);
    	
    		String stackId = null;
    		try {
    			stackId = postStackResponse.getString("id");
    		} catch (JSONException e) {
			e.printStackTrace();
		}
    	
    		String clusterDef = null;   
    		urlString = cloudbreakUrl+"/periscope/v2/clusters";
    		clusterDef = "{\"stackId\": "+stackId+"}";
    	
    		System.out.println("********** Sending Cluster Create request to: "+ urlString +": with payload: "+clusterDef);
        //	JSONObject postClusterResponse = httpPostObject(urlString, clusterDef);
        	//System.out.println(postClusterResponse.toString());
        	
        	ObjectMapper mapper = new ObjectMapper();
    		
    		try {
    			responseMap = mapper.readValue(postStackResponse.toString(), HashMap.class);
    		} catch (JsonParseException e) {
    			e.printStackTrace();
    		} catch (JsonMappingException e) {
    			e.printStackTrace();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    		
    		return responseMap;
    		
    		/*
    		urlString = cloudbreakUrl+cloudbreakApiUriV2+stacksUri;
    		stackDef = "" +
    				"{"
    				+ "\"general\":   {\"credentialName\": \"aws-vvaks\",\"name\": \""+clusterName+"\"},"
    				+ "\"tags\":      {\"userDefinedTags\": {}}, "
    				+ "\"placement\": {\"availabilityZone\": \"us-east-1a\",\"region\": \"us-east-1\"},"
    				+ "\"cluster\": { "
    				+ "		\"fileSystem\": null," 
    				+ "		\"ambari\": {"
    				+ "			\"userName\": \"admin\", "
    				+ "			\"password\": \"admin\"," 
    				+ "     		\"blueprintName\": \"HDP-HDF-MULTI-NODE_V1.1\"," 
    				+ "     		\"ambariRepoDetailsJson\": {" 
    				+ "				\"version\": \"2.5.2.0\","
    				+ "       		\"baseUrl\": \"http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.5.2.0\"," 
    				+ "        		\"gpgKeyUrl\": \"http://public-repo-1.hortonworks.com/ambari/centos6/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins\" "
    				+ "      	}," 
    				+ "      \"ambariStackDetails\": {" 
    				+ "        \"stack\": \"HDP\"," 
    				+ "        \"version\": \"2.6\"," 
    				+ "        \"verify\": false,\n" 
    				+ "        \"stackRepoId\": \"HDP-2.6\",\n" 
    				+ "        \"stackBaseURL\": \"http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.6.2.0\"," 
    				+ "        \"utilsRepoId\": \"HDP-UTILS-1.1.0.21\"," 
    				+ "        \"utilsBaseURL\": \"http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.21/repos/centos6\" " 
    				+ "     }," 
    				+ "		\"gateway\": {\"enableGateway\": false,\"gatewayType\": \"INDIVIDUAL\"}," 
    				+ "		\"validateBlueprint\": false," 
    				+ "     \"ambariSecurityMasterKey\": null" 
    				+ "   }"
    				+ "},"
    				+ "\"instanceGroups\": [" 
    				+ "		{\"parameters\": {},"
    				+ "		 \"template\": {\"parameters\": {\"encrypted\":false},\"instanceType\":\"m4.2xlarge\",\"volumeType\":\"standard\",\"volumeCount\":1,\"volumeSize\":100}," 
    				+ "      \"nodeCount\": 1," 
    				+ "      \"group\": \"master\"," 
    				+ "      \"type\": \"GATEWAY\","
    				+ "      \"recoveryMode\": \"MANUAL\"," 
    				+ "      \"securityGroup\": {\"securityGroupId\":\"sg-6bb0ac22\"}," 
    				+ "      \"recipeNames\": [\"configure-mysql-metastores-v2\"]" 
    				+ "     }," 
    				+ "     {\"parameters\": {}," 
    				+ "      \"template\": {\"parameters\":{\"encrypted\": false},\"instanceType\":\"m4.xlarge\",\"volumeType\":\"standard\",\"volumeCount\":1,\"volumeSize\":100}," 
    				+ "      \"nodeCount\": 1," 
    				+ "      \"group\": \"worker\"," 
    				+ "      \"type\": \"CORE\"," 
    				+ "      \"recoveryMode\": \"MANUAL\","
    				+ "      \"securityGroup\": {\"securityGroupId\":\"sg-7daab634\"}," 
    				+ "      \"recipeNames\": []" 
    				+ "     }"
    				+ " ]," 
    				+ "\"network\":{\"parameters\": {\"vpcId\":\"vpc-d85076bd\",\"subnetId\":\"subnet-dff56386\", \"publicNetId\": null,\"routerId\": null,\"internetGatewayId\": null,\"networkId\": null},\"subnetCIDR\": null},"
    				+ "\"imageSettings\": {\"imageId\":\"cab28152-f5e1-43e1-5107-9e7bbed33eef\",\"imageCatalog\": \"cloudbreak-default\"}," 
    				+ "\"stackAuthentication\": {\"publicKeyId\": \"field\",\"publicKey\": null}," 
    				+ "\"flexId\": null" 
    				+"}";*/
    		
    		//urlString = cloudbreakUrl+cloudbreakApiUri+"/stacks/"+stackId+"/cluster";
    		
    		/*
    		clusterDef = "{\"name\":\""+clusterName+"\","
    			+ "\"blueprintId\":"+blueprintId+","
    			+ "\"enableShipyard\":false,"
    			+ "\"hostGroups\":[{\"name\":\"host_group_1\",\"constraint\":{\"instanceGroupName\":\"host_group_1\",\"hostCount\":1},\"recipeIds\":["+recipeId+"]},"
    							+ "{\"name\":\"host_group_2\",\"constraint\":{\"instanceGroupName\":\"host_group_2\",\"hostCount\":1},\"recipeIds\":[]},"
    							+ "{\"name\":\"host_group_3\",\"constraint\":{\"instanceGroupName\":\"host_group_3\",\"hostCount\":1},\"recipeIds\":["+recipeId+"]}],"
    			+ "\"password\":\"admin\","
    			+ "\"userName\":\"admin\","
    			+ "\"enableSecurity\":false,"
    			+ "\"gateway\":{\"enableGateway\":false,\"exposedServices\":[\"ALL\"]},"
    			+ "\"kerberos\":{\"tcpAllowed\":false},"
    			+ "\"ldapRequired\":false,"
    			+ "\"sssdConfigId\":null,"
    			+ "\"validateBlueprint\":true,"
    			+ "\"fileSystem\":null,"
    			+ "\"customContainer\":null,"
    			+ "\"ambariRepoDetailsJson\":null,"
    			+ "\"ambariStackDetails\":null,"
    			+ "\"configStrategy\":\"ALWAYS_APPLY_DONT_OVERRIDE_CUSTOM_VALUES\"}"; */
    }

	private HashMap<String, Object> getCredentials(String platform) {
    	HashMap<String, Object> credentials = new HashMap<String, Object>();
    	String urlString = cloudbreakUrl+cloudbreakApiUri+credentialsUri;
    	System.out.println("********** " + urlString);
    	JSONArray credentialsJSON = httpGetArray(urlString);
    	for(int i=0;i<credentialsJSON.length();i++){
    		String credential_id;
			try {
				credential_id = credentialsJSON.getJSONObject(i).getString("id");
				String credential_name = credentialsJSON.getJSONObject(i).getString("name");
				String credential_platform = credentialsJSON.getJSONObject(i).getString("cloudPlatform");
				
				System.out.println("**********");
				System.out.println(credentialsJSON);
				System.out.println("**********");
				System.out.println(credential_name);
				System.out.println("**********");
				if(platform.equalsIgnoreCase(credential_platform)){
					System.out.println("**********");
					System.out.println(credential_id+" - "+credential_name+" - "+credential_platform);
					System.out.println("**********");
					credentials.put(credential_name,credential_id);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}	
    	}	
    	return credentials;
    }
    
    private HashMap<String, Object> getTemplates(String platform) {
    	HashMap<String, Object> templates = new HashMap<String, Object>();
    	String urlString = cloudbreakUrl+cloudbreakApiUri+templatesUri;
		
    	JSONArray templatesJSON = httpGetArray(urlString);
    	for(int i=0;i<templatesJSON.length();i++){
    		String template_id;
			try {
				template_id = templatesJSON.getJSONObject(i).getString("id");
				String template_name = templatesJSON.getJSONObject(i).getString("instanceType");
				//String instance_type = templatesJSON.getJSONObject(i).getString("instanceType");
				String template_platform = templatesJSON.getJSONObject(i).getString("cloudPlatform");
				
				//if(template_name.length() <= 25)
				//	template_name = instance_type;
				System.out.println("**********");
				System.out.println(template_name);
				System.out.println("**********");
				if(platform.equalsIgnoreCase(template_platform)){
					System.out.println("**********");
					System.out.println(template_id+" - "+template_name+" - "+template_platform);
					System.out.println("**********");
					templates.put(template_name,template_id);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}	
    	}	
    	return templates;
    }
    
    private HashMap<String, Object> getSecurityGroups(String platform) {
    	HashMap<String, Object> securityGroups = new HashMap<String, Object>();
    	String urlString = cloudbreakUrl+cloudbreakApiUri+securityGroupsUri;
		
    	JSONArray securityGroupsJSON = httpGetArray(urlString);
    	for(int i=0;i<securityGroupsJSON.length();i++){
    		String securityGroup_id;
			try {
				securityGroup_id = securityGroupsJSON.getJSONObject(i).getString("id");
				String securityGroup_name = securityGroupsJSON.getJSONObject(i).getString("name");
				String securityGroup_platform = securityGroupsJSON.getJSONObject(i).getString("cloudPlatform");
				System.out.println("**********");
				System.out.println(securityGroup_name);
				System.out.println("**********");
				if(platform.equalsIgnoreCase(securityGroup_platform)){
					System.out.println("**********");
					System.out.println(securityGroup_id+" - "+securityGroup_name+" - "+securityGroup_platform);
					System.out.println("**********");
					securityGroups.put(securityGroup_name,securityGroup_id);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}	
    	}	
    	return securityGroups;
    }
    
    private HashMap<String, Object> getNetworks(String platform) {
    	HashMap<String, Object> networks = new HashMap<String, Object>();
    	String urlString = cloudbreakUrl+cloudbreakApiUri+networksUri;
		
    	JSONArray networksJSON = httpGetArray(urlString);
    	for(int i=0;i<networksJSON.length();i++){
    		String network_id;
			try {
				network_id = networksJSON.getJSONObject(i).getString("id");
				String network_name = networksJSON.getJSONObject(i).getString("name");
				String network_platform = networksJSON.getJSONObject(i).getString("cloudPlatform");
				System.out.println("**********");
				System.out.println(network_name);
				System.out.println("**********");
				if(platform.equalsIgnoreCase(network_platform)){
					System.out.println("**********");
					System.out.println(network_id+" - "+network_name+" - "+network_platform);
					System.out.println("**********");
					networks.put(network_name,network_id);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}	
    	}	
    	return networks;
    }
    
    private HashMap<String, Object> getRecipes() {
    		HashMap<String, Object> recipes = new HashMap<String, Object>();
    		String urlString = cloudbreakUrl+cloudbreakApiUri+recipesUri;
		
    		JSONArray recipesJSON = httpGetArray(urlString);
    		for(int i=0;i<recipesJSON.length();i++){
    			String recipe_id;
			try {
				recipe_id = recipesJSON.getJSONObject(i).getString("id");
				String recipe_name = recipesJSON.getJSONObject(i).getString("name");
				System.out.println("**********");
				System.out.println(recipe_id+" - "+recipe_name);
				System.out.println("**********");
				recipes.put(recipe_name,recipe_id);
			} catch (JSONException e) {
				e.printStackTrace();
			}	
    		}	
    	
    		return recipes;
    }
    
	private String createSemiEphemeralRecipe(String clusterName, String sourceClusterId, String sourceDatasetName) {
		HashMap<String, Object> recipes = new HashMap<String, Object>();
		String urlString = cloudbreakUrl+cloudbreakApiUri+"/recipes/user";
	
		String recipeContent = "#!/bin/bash \n"+
			 		 "yum install -y wget \n"+
			 		 "git clone https://github.com/vakshorton/CloudBreakArtifacts \n"+
			 		 "CloudBreakArtifacts/recipes/dps_dlm_register_cluster.py false "+dpsHost+" "+sourceClusterId+" "+sourceDatasetName;
	
		String encodedContent = Bytes.toString(Base64.encodeBase64(recipeContent.getBytes())); 
		System.out.println("********** Creating Recipe content: "+recipeContent+": Base64 encoded: "+encodedContent);
	
		String payload = "{\"name\":\""+clusterName+"\",\"description\": \"temporal recipe for ephemeral cluster\",\"recipeType\":\"POST_CLUSTER_INSTALL\",\"content\": \""+encodedContent+"\",\"uri\": null}";
		System.out.println("********** Creating Recipe payload: "+payload);
	
		JSONObject recipesJSON = httpPostObject(urlString, payload);
		String recipe_id = null;
		String recipe_name = null;
		try {
			recipe_id = recipesJSON.getString("id");
			recipe_name = recipesJSON.getString("name");
			System.out.println("********** " +recipe_id+" - "+recipe_name);
			recipes.put(recipe_name,recipe_id);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return recipe_name;
	}
    
    private HashMap<String, Object> createEphemeralRecipe(String clusterName, String sharedServicesIp, String targetBucket, String sharedRangerHiveRepo) {
    		HashMap<String, Object> recipes = new HashMap<String, Object>();
    		String urlString = cloudbreakUrl+cloudbreakApiUri+"/recipes/user";
		
    		String recipeContent = "#!/bin/bash \n"+
				 		 "yum install -y wget \n"+
				 		 "git clone https://github.com/vakshorton/CloudBreakArtifacts \n"+
				 		 "CloudBreakArtifacts/recipes/ephemeral-cluster-install.sh "+sharedServicesIp+" "+targetBucket+" "+sharedRangerHiveRepo;
		
    		String encodedContent = Bytes.toString(Base64.encodeBase64(recipeContent.getBytes())); 
		System.out.println("********** Creating Recipe content: "+recipeContent+": Base64 encoded: "+encodedContent);
    	
		String payload = "{\"name\":\""+clusterName+"\",\"description\": \"temporal recipe for ephemeral cluster\",\"recipeType\":\"POST_CLUSTER_INSTALL\",\"content\": \""+encodedContent+"\",\"uri\": null}";
		System.out.println("********** Creating Recipe payload: "+payload);
		
    		JSONObject recipesJSON = httpPostObject(urlString, payload);
		try {
			String recipe_id = recipesJSON.getString("id");
			String recipe_name = recipesJSON.getString("name");
			System.out.println("********** " +recipe_id+" - "+recipe_name);
			recipes.put(recipe_name,recipe_id);
		} catch (JSONException e) {
			e.printStackTrace();
		}

    		return recipes;
    }
    
    private JSONObject createSharedServicesLdap(String hostName, String hostAddress, String ldapPort) {
    		JSONObject response = null;
    		
    		String urlString = cloudbreakUrl+cloudbreakApiUri+ldapUri;
    		
    		String payload = "{\n" + 
    				"  \"name\": \""+hostName+ldapSuffix+"\",\n" + 
    				"  \"serverHost\": \""+hostAddress+"\",\n" + 
    				"  \"serverPort\": "+ldapPort+",\n" + 
    				"  \"protocol\": \"LDAP\",\n" + 
    				"  \"bindDn\": \"uid=admin,ou=people,dc=hadoop,dc=apache,dc=org\",\n" + 
    				"  \"domain\": \"\",\n" + 
    				"  \"bindPassword\": \"admin-password\",\n" + 
    				"  \"adminGroup\": \"\",\n" + 
    				"  \"userSearchBase\": \"ou=people,dc=hadoop,dc=apache,dc=org\",\n" + 
    				"  \"userObjectClass\": \"person\",\n" + 
    				"  \"userNameAttribute\": \"uid\",\n" + 
    				"  \"groupSearchBase\": \"ou=groups,dc=hadoop,dc=apache,dc=org\",\n" + 
    				"  \"groupObjectClass\": \"groupOfNames\",\n" + 
    				"  \"groupNameAttribute\": \"cn\",\n" + 
    				"  \"groupMemberAttribute\": \"member\",\n" + 
    				"  \"directoryType\": \"LDAP\"\n" + 
    				"}";
    		LOG.info("********** createSharedServiceLDAP() Url: "+urlString+" payload: " + payload);
    		response = httpPostObject(urlString, payload);
    		
    		return response;
    }
    
    private JSONObject createSharedServicesHiveMetastoreRds(String hostName, String hostAddress, String metastorePort) {
		JSONObject response = null;
		
		String urlString = cloudbreakUrl+cloudbreakApiUri+rdsUri;
		
		String payload = "{\n" + 
				"  \"connectionURL\": \"jdbc:postgresql://"+hostAddress+":"+metastorePort+"/hive\",\n" + 
				"  \"name\": \""+hostName+hiveRdsSuffix+"\",\n" + 
				"  \"type\": \"HIVE\",\n" + 
				"  \"databaseEngine\": \"POSTGRES\",\n" +
				"  \"connectorJarUrl\": null,\n" + 
				"  \"connectionUserName\": \"postgres\",\n" + 
				"  \"connectionPassword\": \"postgres\"\n" + 
				"}";
		LOG.info("********** createSharedServiceHiveMetastoreRds() Url: "+urlString+" payload: " + payload);
		response = httpPostObject(urlString, payload);
		
		return response;
    }
    
    private JSONObject createSharedServicesRangerMetastoreRds(String hostName, String hostAddress, String metastorePort) {
		JSONObject response = null;
		
		String urlString = cloudbreakUrl+cloudbreakApiUri+rdsUri;
		
		String payload = "{\n" + 
				"  \"connectionURL\": \"jdbc:postgresql://"+hostAddress+":"+metastorePort+"/ragnger\",\n" + 
				"  \"name\": \""+hostName+rangerRdsSuffix+"\",\n" + 
				"  \"type\": \"RANGER\",\n" + 
				"  \"databaseEngine\": \"POSTGRES\",\n" +
				"  \"connectorJarUrl\": null,\n" + 
				"  \"connectionUserName\": \"rangeradmin\",\n" + 
				"  \"connectionPassword\": \"ranger\"\n" + 
				"}";
		LOG.info("********** createSharedServiceRangerMetastoreRds() Url: "+urlString+" payload: " + payload);
		response = httpPostObject(urlString, payload);
		
		return response;
    }
    
    private Map<String,String> getSharedServicesService() throws JsonParseException, JsonMappingException, IOException, JSONException {
    		JSONArray response = null;
    		String urlString = dpsUrl+dps_clusters_uri;
    		String token = getDpsToken(dpsUrl+dps_auth_uri).get(0);
    		//ObjectMapper mapper = new ObjectMapper();
    		Map<String,String> clusterData = new HashMap<String,String>();
    		boolean isSharedServicesProvisioned = false;
    		response = httpGetDpsArray(urlString, token);
    		 
    		for(int i=0; i<response.length(); i++) {
    			LOG.info("********** getSharedServices(): " + response.getJSONObject(i).getJSONObject("data"));
    			//List<String> tags = mapper.readValue(response.getJSONObject(i).getJSONObject("data").getJSONObject("properties").getJSONArray("tags").toString(), List.class);
    			JSONArray tags = response.getJSONObject(i).getJSONObject("data").getJSONObject("properties").getJSONArray("tags");
    			LOG.info("********** tags: " + tags);
    			for(int j=0; j<tags.length(); j++) {
    				if(tags.getJSONObject(j).getString("name").equalsIgnoreCase("shared-services")){
        				sharedServicesClusterId = response.getJSONObject(i).getJSONObject("data").getString("id");
        				sharedServicesClusterName = response.getJSONObject(i).getJSONObject("data").getString("name");
        				sharedServicesAmbariHost = response.getJSONObject(i).getJSONObject("data").getString("ambariUrl").replace("http://", "").split(":")[0];
        				clusterData.put("id", response.getJSONObject(i).getJSONObject("data").getString("id"));
        				clusterData.put("ambariUrl", response.getJSONObject(i).getJSONObject("data").getString("ambariUrl"));
        				isSharedServicesProvisioned = true;
        				LOG.info("********** sharedServicesClusterName: " + sharedServicesClusterName);
        				break;
        			}
    			}
    		}
    		if(!isSharedServicesProvisioned) {
    			sharedServicesClusterId = null;
			sharedServicesClusterName = null;
			sharedServicesAmbariHost = null;
    		}
    		
    		return clusterData;
    }
    
    private List<String> getDpsToken(String urlString) {
    	List<String> token = null;
    	JSONObject response = null;
    	String payload = "{\"username\":\""+dpsAdminUserName+"\",\"password\":\""+dpsAdminPassword+"\"}";
    	try {
    		URL url = new URL (urlString);
    		LOG.info("**********" + url + " payload: " + payload);
    		HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
    		connection.setDoOutput(true);
    		connection.setRequestMethod("POST");
    		//connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
    		connection.setRequestProperty("Content-Type", "application/json");
    		OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
    		
    		if (connection.getResponseCode() == 401) {
    			LOG.error("Token Rejected, refreshing...");
    			//String credentials = "{\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
    			//oAuthToken = getToken(cloudbreakAuthUrl, credentials);
    			//response = httpPostObject(urlString, payload);
    		}else if (connection.getResponseCode() > 202) {
    			throw new RuntimeException("Failed : HTTP error code : "+ connection.getResponseCode());
    		}else{
    			InputStream content = (InputStream)connection.getInputStream();
    			BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
          		String jsonText = readAll(rd);
          		response = new JSONObject(jsonText);
          		token = connection.getHeaderFields().get("Set-Cookie");
    		}
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	return token;
    }
    
    private int deleteEphemeralRecipe(String clusterName) {
    	String urlString = cloudbreakUrl+cloudbreakApiUri+"/recipes/user/"+clusterName;

    	return httpDeleteObject(urlString);
    }
    
    private JSONObject httpGetObject(String urlString) {
    		JSONObject response = null;
    		URL url;
    		try {
    			url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
            if (connection.getResponseCode() <= 202) {
            		InputStream content = (InputStream)connection.getInputStream();
            		BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
            		String jsonText = readAll(rd);
            		response = new JSONObject(jsonText);
    		  	} else if (connection.getResponseCode() > 202) {	
    		  		response = new JSONObject("{\"input\":\""+urlString+"\",\"result\":\"not-found\"}");
    		  	}
    		} catch (MalformedURLException e) {
				e.printStackTrace();
    		} catch (ProtocolException e) {
				e.printStackTrace();
		} catch (IOException e) {
				e.printStackTrace();
		} catch (JSONException e) {
				e.printStackTrace();
		}
		return response;
    }
    
    private JSONObject httpGetObject(String urlString, String authorizationString, boolean secure) {
    	JSONObject response = null;
    	InputStream content = null;
    	
    	try {
            URL url = new URL (urlString);
            if(secure){
            	HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            	connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.setConnectTimeout(3000);
                connection.setRequestProperty  ("Authorization", authorizationString);
                content = (InputStream)connection.getInputStream();
            }else{
            	HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            	connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.setConnectTimeout(3000);
                connection.setRequestProperty  ("Authorization", authorizationString);
                content = (InputStream)connection.getInputStream();
            }
            
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONObject(jsonText);
        } catch(Exception e) {
            LOG.error("********** Ambari connection timed out: "+urlString);
        }
		return response;
    }
    
    private JSONArray httpGetArray(String urlString) {
    	int responseCode = 0;
    	JSONArray response = null;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
            InputStream content = (InputStream)connection.getInputStream();
    		BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONArray(jsonText);
        } catch(Exception e) {
        	if(true){
        		LOG.error("Token Rejected, refreshing...");
    			String credentials = "credentials={\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
    			oAuthToken = getToken(cloudbreakAuthUrl, credentials);
    			response = httpGetArray(urlString);
    		}else{
    			e.printStackTrace();
    		}
        }
		return response;
    }
    
    private JSONArray httpGetArray(String urlString, String authorizationString) {
    	JSONArray response = null;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", authorizationString);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
    
    private JSONObject httpPostObject(String urlString, String payload) {
    	JSONObject response = null;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
            connection.setRequestProperty("Content-Type", "application/json");
            
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
    		if (connection.getResponseCode() == 401) {
    			LOG.error("Token Rejected, refreshing...");
    			String credentials = "credentials={\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
    			oAuthToken = getToken(cloudbreakAuthUrl, credentials);
    			response = httpPostObject(urlString, payload);
    		}else if (connection.getResponseCode() > 202) {
    			throw new RuntimeException("Failed : HTTP error code : "+ connection.getResponseCode());
    		}else{
    			InputStream content = (InputStream)connection.getInputStream();
    			BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      		String jsonText = readAll(rd);
  	      		response = new JSONObject(jsonText);
    		}
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
    
	private int httpDeleteObject(String urlString) {
		int responseCode = 0;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
            InputStream content = (InputStream)connection.getInputStream();
            //BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	//String jsonText = readAll(rd);
            if (connection.getResponseCode() == 401) {
    			LOG.error("Token Rejected, refreshing...");
    			String credentials = "credentials={\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
    			oAuthToken = getToken(cloudbreakAuthUrl, credentials);
    			responseCode = httpDeleteObject(urlString);
    		}else{
  	      		responseCode = connection.getResponseCode();
    		}	
        } catch(Exception e) {
            e.printStackTrace();
        }
		return responseCode;
	}
	
	private static String getToken(String urlString, String credentials) {
    	String response = null;
    	
    	try {
            URL url = new URL (urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept","application/x-www-form-urlencoded");
            
            OutputStream os = connection.getOutputStream();
    		os.write(credentials.getBytes());
    		os.flush();
    		
            if (connection.getResponseCode() != 302) {
    			throw new RuntimeException("Failed : HTTP error code : "+ connection.getResponseCode());
    		}
            
            //System.out.println(connection.getResponseMessage());
    		String[] responseArray = connection.getHeaderField("Location").split("access_token=")[1].split("&");
    		System.out.println(responseArray[0]);
    		//System.out.println(responseArray[1].split("=")[1]);
            response = responseArray[0];
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
	
	private String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
	
	private int getCurrentHour() {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date); 
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	private int getCurrentDay() {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date); 
		return calendar.get(Calendar.DAY_OF_YEAR);
	}
	
	private int getCurrentYear() {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date); 
		return calendar.get(Calendar.YEAR);
	}
	
	private void initializeTrustManager(){
		TrustManager[] trustAllCerts = new TrustManager[]{
			new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
				public void checkClientTrusted(
					java.security.cert.X509Certificate[] certs, String authType) {
				}
				public void checkServerTrusted(
					java.security.cert.X509Certificate[] certs, String authType) {
				}
			}
		};

		//Install the all-trusting trust manager
		try {
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
		    };
			
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
