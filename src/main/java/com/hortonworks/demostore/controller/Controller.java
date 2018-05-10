package com.hortonworks.demostore.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
	private String sharedServicesAmabriHost;
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

	private String oAuthToken = null;
	private String s3ArnRoleInstanceProfile = "arn:aws:iam::081339556850:instance-profile/shared-services-s3-access";
	
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
        if(env.get("ATLAS_HOST") != null){
        	atlasApiHost = (String)env.get("ATLAS_HOST");
        }
        if(env.get("ATLAS_PORT") != null){
        	atlasApiPort = (String)env.get("ATLAS_PORT");
        }
        if(env.get("SHARED_SERVICES_AMBARI_HOST") != null){
        	sharedServicesAmabriHost = (String)env.get("SHARED_SERVICES_AMBARI_HOST");
        }
        
        atlasUrl = "http://"+atlasApiHost+":"+atlasApiPort;
        cloudbreakUrl = "https://"+cloudbreakApiHost+":"+cloudbreakApiPort;
        cloudbreakAuthUrl = "http://"+cloudbreakAuthHost+":"+cloudbreakAuthPort+"/oauth/authorize?response_type=token&client_id=cloudbreak_shell";
        System.out.println("********************** Controller ()  Shared Services Amabri : " + sharedServicesAmabriHost);
        System.out.println("********************** Controller ()  Cloudbreak API url set : " + cloudbreakUrl);
        System.out.println("********************** Controller ()  Cloudbreak Auth url set : " + cloudbreakAuthUrl);
        initializeTrustManager();
        
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
		 }
    }
	
	@RequestMapping(value="/refreshAllClusters", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object> getClusterStatus() {
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
	    for(int i=0;i<clustersJSON.length();i++){
	    	HashMap<String, Object> cluster = new HashMap<String,Object>();
	    	String clusterType = null;
	    	try {
	    		String blueprintName = clustersJSON.getJSONObject(i).getJSONObject("cluster").getJSONObject("blueprint").getString("name");
	    		if(blueprintName != null){
	    			if(blueprintName.split("_v")[0].equalsIgnoreCase("hdp-hdf-multi-node") || (blueprintName.split("_V")[0].equalsIgnoreCase("hdp-hdf-multi-node"))){
	    				clusterType = "Connected Platform";
	    			}else if(blueprintName.split("_v")[0].equalsIgnoreCase("historian-multi-node") || blueprintName.split("_v")[0].equalsIgnoreCase("historian-multi-node-scalable")){
	    				clusterType = "Historian";
	    			}else if (blueprintName.split("_v")[0].equalsIgnoreCase("shared-services") || (blueprintName.split("-V")[0].equalsIgnoreCase("shared-services"))){
	    				clusterType = "Shared Services";  		
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
		System.out.println("********** Sending Cluster Terminate request to: "+ urlString);
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

    	data = callAtlas("http://172.26.247.24:21000/api/atlas/v1/taxonomies/", restTemplate, data);
    	
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
			JSONArray entitiesArray = atlasClient2.searchByDSL("hive_table where name = "+entityName, -1, 0);
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
    	for(int i=0;i<blueprintsJSON.length();i++){
			try {
				JSONObject blueprint = blueprintsJSON.getJSONObject(i); 
	    		String[] blueprint_name = blueprint.getString("name").split("_v");
    			System.out.println("**********");
    			System.out.println(blueprint);
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
  			        	blueprints.put(blueprint_name[0],blueprint.getInt("id"));
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
    		@RequestParam(value="targetBucket") String targetBucket) {
    	
    	HashMap<String, Object> responseMap = null;
    	
    	String urlString = cloudbreakUrl+cloudbreakApiUri+stacksUri;
    	
    	String region = null;
		String zone= null;
		String variant= null;
		String securityGroupId = null;
		String networkId = null;
		String blueprintId = null;
		String recipeId = null;
    	
		//credentialId = (String) ((HashMap)platformMetaData.get(platform).get("credentials")).get(credential);
		//templateId = (String) ((HashMap)platformMetaData.get(platform).get("templates")).get(template);
		
    	if (platform.equalsIgnoreCase("AWS")){
    		  region="us-east-1";
    		  zone="null";
    		  variant="AWS";
    		  securityGroupId = (String) ((HashMap)platformMetaData.get(platform).get("securityGroups")).get("aws-connected-platform-all-services-v1");
    		  networkId = (String) ((HashMap)platformMetaData.get(platform).get("networks")).get("aws-existing-vpc-subnet");
    	}else if (platform.equalsIgnoreCase("OPENSTACK")){
    		  region="RegionOne";
    		  zone="\"SE\"";
    		  variant="HEAT";
    		  securityGroupId = (String) ((HashMap)platformMetaData.get(platform).get("securityGroups")).get("openstack-connected-platform-demo-all-services-port-v2");
    		  networkId = (String) ((HashMap)platformMetaData.get(platform).get("networks")).get("fieldcloud-openstack-network");
    	}else{
    		System.out.println("********************Invalid cloud platform requested...");
    		System.out.println("********************Valid platform types are: AWS, OPENSTACK");
    		return null;
    	}
    	
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
    	
    	
    	String stackDef = null;
    if (type.equalsIgnoreCase("shared-services")){    
    		urlString = cloudbreakUrl+cloudbreakApiUriV2+stacksUri;
    		
    		if(platform.equalsIgnoreCase("OPENSTACK")) {
    			stackDef = "" +
    				"{\n" + 
    				"  \"general\": {\n" + 
    				"    \"credentialName\": \"openstack\",\n" + 
    				"    \"name\": \""+clusterName+"\"\n" + 
    				"  },\n" + 
    				"  \"placement\": {\n" + 
    				"    \"region\": \"RegionOne\",\n" + 
    				"    \"availabilityZone\": \"SE\"\n" + 
    				"  },\n" + 
    				"  \"tags\": {\n" + 
    				"    \"userDefinedTags\": {\n" + 
    				"      \n" + 
    				"    }\n" + 
    				"  },\n" + 
    				"  \"cluster\": {\n" + 
    				"    \"ambari\": {\n" + 
    				"      \"blueprintName\": \"shared-services\",\n" + 
    				"      \"platformVersion\": \"HDP 2.6\",\n" + 
    				"      \"ambariRepoDetailsJson\": {\n" + 
    				"        \"version\": \"2.6.1.3\",\n" + 
    				"        \"baseUrl\": \"http:\\/\\/public-repo-1.hortonworks.com\\/ambari\\/centos7\\/2.x\\/updates\\/2.6.1.3\",\n" + 
    				"        \"gpgKeyUrl\": \"http:\\/\\/public-repo-1.hortonworks.com\\/ambari\\/centos7\\/RPM-GPG-KEY\\/RPM-GPG-KEY-Jenkins\"\n" + 
    				"      },\n" + 
    				"      \"ambariStackDetails\": {\n" + 
    				"        \"version\": \"2.6\",\n" + 
    				"        \"verify\": false,\n" + 
    				"        \"enableGplRepo\": false,\n" + 
    				"        \"stack\": \"HDP\",\n" + 
    				"        \"repositoryVersion\": \"2.6.4.5-2\",\n" + 
    				"        \"versionDefinitionFileUrl\": \"http:\\/\\/private-repo-1.hortonworks.com\\/HDP\\/centos7\\/2.x\\/updates\\/2.6.4.5-2\\/HDP-2.6.4.5-2.xml\",\n" + 
    				"        \"stackOs\": \"centos7\",\n" + 
    				"        \"mpacks\": [\n" + 
    				"          \n" + 
    				"        ]\n" + 
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
    				"    \"rdsConfigNames\": [\n" + 
    				"      \n" + 
    				"    ],\n" + 
    				"    \"ldapConfigName\": null,\n" + 
    				"    \"proxyName\": null\n" + 
    				"  },\n" + 
    				"  \"flexId\": null,\n" + 
    				"  \"imageSettings\": {\n" + 
    				"    \"imageCatalog\": \"cloudbreak-default\",\n" + 
    				"    \"imageId\": \"74d99079-f5d0-4470-78b3-11ceaf944069\"\n" + 
    				"  },\n" + 
    				"  \"imageType\": \"base\",\n" + 
    				"  \"instanceGroups\": [\n" + 
    				"    {\n" + 
    				"      \"parameters\": {\n" + 
    				"        \n" + 
    				"      },\n" + 
    				"      \"template\": {\n" + 
    				"        \"parameters\": {\n" + 
    				"          \"encrypted\": false\n" + 
    				"        },\n" + 
    				"        \"instanceType\": \"m3.xlarge\",\n" + 
    				"        \"volumeType\": \"HDD\",\n" + 
    				"        \"volumeCount\": 0,\n" + 
    				"        \"volumeSize\": 100\n" + 
    				"      },\n" + 
    				"      \"nodeCount\": 1,\n" + 
    				"      \"group\": \"master\",\n" + 
    				"      \"type\": \"GATEWAY\",\n" + 
    				"      \"recoveryMode\": \"MANUAL\",\n" + 
    				"      \"recipeNames\": [\n" + 
    				"        \"configure-mysql-metastores\",\n" + 
    				"        \"dps-dlm-register-cluster\",\n" + 
    				"        \"dps-dlm-remove-cluster\",\n" + 
    				"        \"install-dps-agents\"\n" + 
    				"      ],\n" + 
    				"      \"securityGroup\": {\n" + 
    				"        \"securityGroupId\": \"de632b11-944c-4acb-a0cd-47f864150d5e\"\n" + 
    				"      }\n" + 
    				"    },\n" + 
    				"    {\n" + 
    				"      \"parameters\": {\n" + 
    				"        \n" + 
    				"      },\n" + 
    				"      \"template\": {\n" + 
    				"        \"parameters\": {\n" + 
    				"          \"encrypted\": false\n" + 
    				"        },\n" + 
    				"        \"instanceType\": \"m3.xlarge\",\n" + 
    				"        \"volumeType\": \"HDD\",\n" + 
    				"        \"volumeCount\": 0,\n" + 
    				"        \"volumeSize\": 100\n" + 
    				"      },\n" + 
    				"      \"nodeCount\": 1,\n" + 
    				"      \"group\": \"worker\",\n" + 
    				"      \"type\": \"CORE\",\n" + 
    				"      \"recoveryMode\": \"MANUAL\",\n" + 
    				"      \"recipeNames\": [\n" + 
    				"        \n" + 
    				"      ],\n" + 
    				"      \"securityGroup\": {\n" + 
    				"        \"securityGroupId\": \"de632b11-944c-4acb-a0cd-47f864150d5e\"\n" + 
    				"      }\n" + 
    				"    }\n" + 
    				"  ],\n" + 
    				"  \"network\": {\n" + 
    				"    \"parameters\": {\n" + 
    				"      \"publicNetId\": null,\n" + 
    				"      \"routerId\": null,\n" + 
    				"      \"internetGatewayId\": null,\n" + 
    				"      \"subnetId\": \"aa7c8bb9-0152-46b9-8596-935baca704a0\",\n" + 
    				"      \"networkingOption\": \"provider\",\n" + 
    				"      \"networkId\": \"71a870bb-191c-4abe-bf02-ece2e9b3345c\"\n" + 
    				"    },\n" + 
    				"    \"subnetCIDR\": null\n" + 
    				"  },\n" + 
    				"  \"stackAuthentication\": {\n" + 
    				"    \"publicKeyId\": \"Field\",\n" + 
    				"    \"publicKey\": null\n" + 
    				"  }\n" + 
    				"}";
    		}else if(variant.equalsIgnoreCase("aws")) {
    			stackDef = "" +
    				"{"
    				+ "\"general\":   {\"credentialName\": \"openstack\",\"name\": \""+clusterName+"\"},"
    				+ "\"tags\":      {\"userDefinedTags\": {}}, "
    				+ "\"placement\": {\"availabilityZone\": \"us-east-1a\",\"region\": \"us-east-1\"},"
    				+ "\"cluster\": { "
    				+ "		\"fileSystem\": null," 
    				+ "		\"ambari\": {"
    				+ "			\"userName\": \"admin\", "
    				+ "			\"password\": \"admin\"," 
    				+ "     		\"blueprintName\": \"SHARED-SERVICES-V1.9\"," 
    				+ "     		\"ambariStackDetails\": {\"stack\": \"HDP\",\"version\": \"2.6\",\"verify\": false,\"enableGplRepo\": false}," 
    				+ "     		\"gateway\": {\"enableGateway\": false,\"gatewayType\": \"INDIVIDUAL\"}," 
    				+ "			\"validateBlueprint\": false," 
    				+ "      	\"ambariSecurityMasterKey\": null" 
    				+ "    	}"
    				+ "},"
    				+ "\"instanceGroups\": [" 
    				+ "		{\"parameters\": {},"
    				+ "		 \"template\": {\"parameters\": {\"encrypted\":false},\"instanceType\":\"m4.2xlarge\",\"volumeType\":\"standard\",\"volumeCount\":1,\"volumeSize\":100}," 
    				+ "      \"nodeCount\": 1," 
    				+ "      \"group\": \"master\"," 
    				+ "      \"type\": \"GATEWAY\","
    				+ "      \"recoveryMode\": \"MANUAL\"," 
    				+ "      \"securityGroup\": {\"securityGroupId\":\"sg-6bb0ac22\"}," 
    				+ "      \"recipeNames\": [\"configure-mysql-metastores-v2\",\"install-dps-agents\",\"register-cluster-dps-dlm-v2\",\"remove-cluster-dps-dlm\"]" 
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
    				+ "\"imageSettings\": {\"imageId\":\"433025f0-16bb-4b83-5ef7-fb795a9fc8ab\",\"imageCatalog\": \"cloudbreak-default\"}," 
    				+ "\"stackAuthentication\": {\"publicKeyId\": \"field\",\"publicKey\": null}," 
    				+ "\"flexId\": null" 
    				+"}";
    		}
    		/*stackDef = ""
    		+ "{\"name\":\""+clusterName+"\","
    		+ "\"credentialId\":"+credentialId+","
    		+ "\"region\":\""+region+"\","
    		+ "\"failurePolicy\":{\"adjustmentType\":\"BEST_EFFORT\",\"threshold\":null},"
    		+ "\"onFailureAction\":\"DO_NOTHING\","
    		+ "\"instanceGroups\":[{\"templateId\":"+templateId+",\"securityGroupId\":"+securityGroupId+",\"group\":\"master\",\"nodeCount\":1,\"type\":\"GATEWAY\"},"
    					 		+ "{\"templateId\":"+templateId+",\"securityGroupId\":"+securityGroupId+",\"group\":\"worker\",\"nodeCount\":1,\"type\":\"CORE\"},"
    					 		+ "\"parameters\":{},"
    					 		+ "\"parameters\":{\"instanceProfileStrategy\":\"USE_EXISTING\",\"instanceProfile\":\""+s3ArnRoleInstanceProfile+"\"},"
    					 		+ "\"networkId\":"+networkId+","
    					 		+ "\"relocateDocker\":false,"
    					 		+ "\"availabilityZone\":"+zone+","
    					 		+ "\"orchestrator\":{\"type\":\"SALT\"},"
    					 		+ "\"tags\":{\"userDefined\":{}},"
    					 		+ "\"platformVariant\":\""+variant+"\",\"customImage\":null}"; */
    	}else {
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
    				+"}";
    		
    	}
    	System.out.println("********** Stack Def:" + stackDef + " to " + urlString);
    	JSONObject postStackResponse = httpPostObject(urlString, stackDef); 
    	System.out.println("********** Response:" + postStackResponse);
    	
    	String stackId = null;
    	try {
		stackId = postStackResponse.getString("id");
	} catch (JSONException e) {
		e.printStackTrace();
	}
    	
    	urlString = cloudbreakUrl+cloudbreakApiUri+"/stacks/"+stackId+"/cluster";
    	
    	urlString = cloudbreakUrl+"/periscope/v1/clusters";
	
    	
    	String clusterDef = null;
    	if (type.equalsIgnoreCase("shared-services")){   
    		urlString = cloudbreakUrl+"/periscope/v1/clusters";
    		clusterDef = "{\"stackId\": "+stackId+"}";
    	}else {
    		clusterDef = "{\"stackId\": "+stackId+"}";
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
    		
    		System.out.println("********** Sending Cluster Create request to: "+ urlString +": with payload: "+clusterDef);
        	JSONObject postClusterResponse = httpPostObject(urlString, clusterDef);
        	System.out.println(postClusterResponse.toString());
        	
        	ObjectMapper mapper = new ObjectMapper();
    		// convert JSON string to Map
    		
    		try {
    			responseMap = mapper.readValue(postStackResponse.toString(), HashMap.class);
    		} catch (JsonParseException e) {
    			e.printStackTrace();
    		} catch (JsonMappingException e) {
    			e.printStackTrace();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    	
    
    	return responseMap;
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
    
    private int deleteEphemeralRecipe(String clusterName) {
    	String urlString = cloudbreakUrl+cloudbreakApiUri+"/recipes/user/"+clusterName;

    	return httpDeleteObject(urlString);
    }
    
    private JSONObject httpGetObject(String urlString) {
    	JSONObject response = null;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONObject(jsonText);
        } catch(Exception e) {
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
