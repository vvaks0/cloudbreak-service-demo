package com.hortonworks.demostore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.hortonworks.demostore.domain.AppProps;
import com.hortonworks.demostore.model.Atlas;


@SpringBootApplication
public class DemoStore {
	
	public static void main(String[] args) {
		ApplicationContext app = SpringApplication.run(DemoStore.class, args);
		
		AppProps props = (AppProps) app.getBean(AppProps.class);
		System.out.println(props.getHostName());
		
		System.out.println(props.getCloudbreakApiHost());
		System.out.println(props.getCloudbreakApiPort());
		
		System.out.println(props.getAltasHost());
		System.out.println(props.getAltasPort());
		
		Atlas.init();
	}
}
