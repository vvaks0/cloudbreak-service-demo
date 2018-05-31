package com.hortonworks.demostore.model;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
//import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import scala.util.parsing.json.JSON;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public abstract class Atlas {
	
	private static String[] atlasBasicAuth = {"admin", "admin"};
	
	private static String adminUserName = "admin";
	private static String adminPassword = "admin";
	private static String ambariAdminUser = "admin";
	private static String ambariAdminPassword = "admin";
	
	private static String dpsHost = "";
	private static String dps_url = "";
	private static String dps_auth_uri = "/auth/in";
	private static String dps_clusters_uri = "/api/actions/clusters?type=all";
	private static String dlm_clusters_uri = "/dlm/api/clusters";
	private static String dlm_policies_uri = "/dlm/api/policies?numResults=200&instanceCount=10";
	private static String dss_collections_uri = "/dss/api/dataset/list/tag/ALL?offset=0&size=10";
	private static String dss_assets_uri = "/dss/api/dataset";	
	
	public static List<AtlasItem> atlasCache = new ArrayList<AtlasItem>();
	public static HashMap<String, Object> atlasFileTree = new HashMap<String, Object>();
	public static String atlasHost = "localhost";
	public static String atlasPort = "21000";
	public static String server = "http://"+atlasHost+":"+atlasPort;
	
	public static String api = "/api/atlas";

	private static final Logger LOG = Logger.getLogger(Atlas.class);
	
	public static void init() {
		Map<String, String> env = System.getenv();
        System.out.println("********************** ENV: " + env);
        if(env.get("ATLAS_HOST") != null){
        	atlasHost = (String)env.get("ATLAS_HOST");
        }
        if(env.get("ATLAS_PORT") != null){
        	atlasPort = (String)env.get("ATLAS_PORT");
        }
        if(env.get("DPS_HOST") != null){
        	dpsHost = (String)env.get("DPS_HOST");
        }
        server = "http://"+atlasHost+":"+atlasPort;
        dps_url = "https://"+dpsHost;
		atlasFileTree = getFileTree();
		//setCache();
	}
   
	
	public static void setCache() {
		HashMap<String, Object> hm = getFileTree();
		
		for (String key: hm.keySet()) {
			RestTemplate rt = new RestTemplate();
	    	rt.getInterceptors().add(
	    			  new BasicAuthorizationInterceptor("admin", "admin"));  	
	    	String entityURL = server+api+"v2/search/dsl?query=`"+key+"`";  //api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
	    	
	    	rt.setMessageConverters(Arrays.asList(new MappingJackson2HttpMessageConverter()));
	    	
	    	//org.apache.atlas.catalog.query.AtlasTaxonomyQuery
	    	
	        ResponseEntity<Object> response = rt.getForEntity(entityURL, Object.class);
	        System.out.println(response.getBody());
	    	
	    	
		}
		
		//TODO code to merge and create cache
	}
	
    public static HashMap<String, Object>  getFileTree() {
    
    	HashMap<String, Object> coreHm = new HashMap<String, Object>();
    	HashMap<String, Object> dataHm = new HashMap<String, Object>();
    	List<Object> data = new ArrayList<Object>();
    	//data.add("Empty Folder");    	
       	
    	RestTemplate restTemplate = new RestTemplate();
    	
    	restTemplate.getInterceptors().add(new BasicAuthorizationInterceptor("admin", "admin"));  	

    	//data = callAtlas(server+api+"/v1/taxonomies/", restTemplate, data);    	
    	
    	dataHm.put("data", data);
    	coreHm.put("core", dataHm);   	
    	    	
    	//return coreHm;
    	String token = getToken(dps_url+dps_auth_uri).get(0);
    	
    	try {
			coreHm = (HashMap<String, Object>) getDssCollectionsTree(token);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return coreHm;
    }
    
    private static Map<String, Object> getDssCollectionsTree(String token) throws JSONException{
		List<Object> collectionList = new ArrayList<Object>();
		
		JSONArray collections = httpGetArray(dps_url+dss_collections_uri, token);
		LOG.info("+++++++++++++ " + collections);
		for(int i=0; i < collections.length(); i++) {
			Map<String, Object> collectionMap = new HashMap<String, Object>();
			List<Object> assetList = new ArrayList<Object>();
			String collectionId = collections.getJSONObject(i).getJSONObject("dataset").getString("id");
			String collectionName = collections.getJSONObject(i).getJSONObject("dataset").getString("name");
			String collectionClusterId = collections.getJSONObject(i).getString("cluster");
			
			collectionMap.put("id", collectionId);
			collectionMap.put("text", collectionName);
			collectionMap.put("clusterId", collectionClusterId);
		
			LOG.info("++++++++++++++ Collection: " + collectionName);
			//JSONArray assets = httpGetArray(dps_url+dss_assets_uri + "/" + collectionId + "/assets?queryName&offset=0&limit=100", token);
			JSONArray assets = httpGetObject(dps_url+dss_assets_uri + "/" + collectionId + "/assets?queryName&offset=0&limit=100", token).getJSONArray("assets");
			for(int j = 0; j < assets.length(); j++) {
				Map<String, Object> asset = new HashMap<String, Object>();
				JSONObject assetJSON = assets.getJSONObject(j);
				String assetId = assetJSON.getString("id");
				String assetName = assetJSON.getString("assetName");
				String assetFQN = assetJSON.getJSONObject("assetProperties").getString("qualifiedName");
				String assetGuid = assetJSON.getString("guid");
				asset.put("id", assetFQN+"*"+assetId+"*"+assetGuid+"*"+collectionId);
				asset.put("text", assetName);
				asset.put("clusterId", collectionClusterId);
				LOG.info("+++++++++++++ AssetId: " + assetId + " AssetName: " + assetName + " GUID: " + assetGuid + " collectionId:" + collectionId);
				assetList.add(asset);
			}
			collectionMap.put("children", assetList);
			collectionList.add(collectionMap);
		}
		
		Map<String, Object> core = new HashMap<String, Object>();
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("data", collectionList);
		core.put("core", data);
		LOG.info("***************** " + core);
		
		return core;
	}
    
    public static Referenceable getEntityByName(){
    	
    	
    		return null;
    }
    
    private static JSONObject httpGetObject(String urlString, String token) {
	    JSONObject response = null;
	    String userpass = ambariAdminUser + ":" + ambariAdminPassword;
    String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
		try {
        URL url = new URL (urlString);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);            
        connection.setRequestProperty ("Authorization", basicAuth);
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

private static JSONObject httpGetObjectAmbari(String urlString) {
    JSONObject response = null;
    String userpass = ambariAdminUser + ":" + ambariAdminPassword;
    String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
	try {
		URL url = new URL (urlString);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setDoOutput(true);            
		connection.setRequestProperty ("Authorization", basicAuth);
		InputStream content = (InputStream)connection.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
      	String jsonText = readAll(rd);
      	response = new JSONObject(jsonText);
	} catch(Exception e) {
		e.printStackTrace();
	}
	return response;
}

private static JSONArray httpGetArray(String urlString, String token) {
	JSONArray response = null;
	try {
        URL url = new URL (urlString);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
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

private JSONObject httpPostObject(String urlString, String payload, String token) {
		JSONObject response = null;
		try {
        URL url = new URL (urlString);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty  ("Cookie", token);
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
		}
    } catch(Exception e) {
        e.printStackTrace();
    }
	return response;
}

private static List<String> getToken(String urlString) {
	List<String> token = null;
	JSONObject response = null;
	String payload = "{\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
	try {
		URL url = new URL (urlString);
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
	
private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
}

private static void initializeTrustManager(){
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
    
	private static List<Object> callAtlas(String url, RestTemplate rt, List<Object> data) {
		// rt.setMessageConverters(Arrays.asList(new
		// MappingJackson2HttpMessageConverter()));
		ResponseEntity<List> response = rt.getForEntity(url, List.class);
		System.out.println(response.getBody());

		for (Object entry : response.getBody()) {
			Map e = (Map) entry;

			String urlTax = (String) e.get("href");
			HashMap<String, Object> taxfolder = new HashMap<String, Object>();
			String tax = (String) e.get("name");
			taxfolder.put("text", tax);
			data.add(taxfolder);
			//List<Object> children = new ArrayList<Object>();
			//folder.put("children", children);

			response = rt.getForEntity(urlTax, List.class);
			List<Object> termChildren = new ArrayList<Object>();
			HashMap<String, Object> prevTermFolder = new HashMap<String, Object>();
			String previousTerm = "_";
			for (Object taxonomy : response.getBody()) {
				Map ta = (Map) taxonomy;

				if (ta.containsKey("terms")) {
					String urlTerm = (String) ((Map) ta.get("terms")).get("href");

					response = rt.getForEntity(urlTerm, List.class);
					for (Object term : response.getBody()) {
						Map te = (Map) term;

						
						//HashMap<String, Object> termFolder = new HashMap<String, Object>();
						String termName = (String) te.get("name");
						HashMap<String, Object> termFolder = new HashMap<String, Object>();
						//termFolder.put("text", termName);
						
						
						if (termName.contains(previousTerm)) {
							//breadcrumb logic, flatten to hierarchy
							termFolder.put("text", termName.replace(previousTerm, "").replace(".", ""));
							List<Object> termSubChildren = new ArrayList<Object>();
							termSubChildren.add(termFolder);
							prevTermFolder.put("children", termSubChildren);
							
						}
						else {
							// add to parent tax folder
							termFolder.put("text", termName.replace(tax, "").replace(".", ""));
							termChildren.add(termFolder);
							taxfolder.put("children", termChildren);
						}
						
						prevTermFolder = termFolder;
						previousTerm = termName;
						
						
						String entityURL = server + api + "/v2/search/dsl?query=`" + (String) te.get("name") + "`"; // api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
						System.out.println(entityURL);

						ResponseEntity<String> response2 = rt.getForEntity(entityURL, String.class);

						JSONParser parser = new JSONParser();
						JSONObject json = null;
						try {
							json = (JSONObject) parser.parse(response2.getBody());
						} catch (ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

						/*
						 * { "queryType": "DSL", "queryText":
						 * "`Catalog.Mining.PA.MineA.TruckA`", "entities": [ {
						 * "typeName": "historian_tag",
						 */
						System.out.println(response2.getBody());

						// Map json = (Map) response2.getBody();
						List<Map> entities = null;
						try {
							entities = (List) json.get("entities");
						} catch (JSONException e1) {
							e1.printStackTrace();
						}
						List<Object> entityChild = new ArrayList<Object>();
						for (Map ent : entities) {
							// System.out.println(ent);
							String typeName = (String) ent.get("typeName");
							if ("historian_tag".equalsIgnoreCase(typeName) || "hive_table".equalsIgnoreCase(typeName)) {
								String guid = (String) ent.get("guid");
								String displayText = (String) ent.get("displayText");
								System.out.println(displayText);

								
								
								HashMap<String, Object> entityfolder = new HashMap<String, Object>();
								entityfolder.put("text", displayText);
								
								
								entityChild.add(entityfolder);
								
								// add to parent folder
								termFolder.put("children", entityChild);
								
								

							}
						}
					}
				}
			}
		}

		return data;

	}

    private static List<Object> callAtlas2 (String url, RestTemplate rt, List<Object> data) {
    	//rt.setMessageConverters(Arrays.asList(new MappingJackson2HttpMessageConverter()));
        ResponseEntity<List> response = rt.getForEntity(url, List.class);
        System.out.println(response.getBody());
        
        for (Object entry :  response.getBody() ) {
        	Map e = (Map) entry;
        	//System.out.println(e +"  "+ e.get("href"));
        	if (e.containsKey("terms")) {
        		url = (String) ((Map) e.get("terms")).get("href");
        		callAtlas(url, rt, data);
        	}
        	else {
        		url = (String)e.get("href");
        		HashMap<String, Object> folder = new HashMap<String, Object>();
        		String term = (String) e.get("name");
        		folder.put("text", term);
        		
        		data.add(folder);
        		
        		List<Object> children = new ArrayList<Object>();
        		folder.put("children", children);
        		
        		if (!"Catalog".equalsIgnoreCase(term)) {

        		// for each term do the SQL search 
 
        		String entityURL = server+api+"/v2/search/dsl?query=`"+(String) e.get("name")+"`";  //api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
    	    	System.out.println(entityURL);
    	    	
    	        ResponseEntity<String> response2 = rt.getForEntity(entityURL, String.class);
    	       
    	        JSONParser parser = new JSONParser();
    	        JSONObject json = null;
				try {
					json = (JSONObject) parser.parse(response2.getBody());
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

    	       
           		/*{
    			"queryType": "DSL",
    			"queryText": "`Catalog.Mining.PA.MineA.TruckA`",
    			"entities": [
    			   {
    			"typeName": "historian_tag",
    		*/
    	        System.out.println(response2.getBody());
    	      
    	        //Map json = (Map) response2.getBody();
    	        List<Map> entities = null;
				try {
					entities = (List) json.get("entities");
				} catch (JSONException e1) {
					e1.printStackTrace();
				}
    	        for (Map ent: entities) {
    	        	//System.out.println(ent);
    	        	String typeName = (String) ent.get("typeName");
    	        	if ("historian_tag".equalsIgnoreCase(typeName)) {
    	        		String guid = (String) ent.get("guid");
    	        		String displayText = (String) ent.get("displayText");
    	        		System.out.println(displayText);
    	        		
    	        		
    	        		folder = new HashMap<String, Object>();
    	        		
    	        		folder.put("text", displayText);
    	        		
    	        		children.add(folder);
    	        		
    	        		
    	        		
    	        	}
    	        }
        		
        	}
        		
        		callAtlas(url, rt, children);
        	}
        		
        	
        }
        // need exit condition
		return data;
    }
    
}
