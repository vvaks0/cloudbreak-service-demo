package com.hortonworks.demostore.controller;

import javax.servlet.http.HttpSession;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;


@org.springframework.stereotype.Controller
class ControllerView extends WebMvcConfigurerAdapter {

	@Override
	public void addViewControllers(ViewControllerRegistry registry) {
		
		registry.addViewController("/").setViewName("theme1/index");
		registry.addViewController("/register").setViewName("theme1/register");
		registry.addViewController("/signin").setViewName("theme1/signin");
	}

	@RequestMapping(value = "/index", produces = { MediaType.TEXT_HTML_VALUE }, method = RequestMethod.GET)
	public String index() {
		return "theme1/index";
	}

	@RequestMapping(value = "/logout", produces = { MediaType.TEXT_HTML_VALUE }, method = RequestMethod.GET)
	public String logout(HttpSession session) {
		session.invalidate();
		return "theme1/index";
	}

	@RequestMapping(value = "/error", produces = { MediaType.TEXT_HTML_VALUE }, method = RequestMethod.GET)
	public String errorPage() {
		return "theme1/error";
	}

	@RequestMapping(value = "/signin", produces = { MediaType.TEXT_HTML_VALUE }, method = RequestMethod.GET)
	public String viewsignin() {
		return "theme1/signin";
	}

	@RequestMapping(value = "/register", produces = { MediaType.TEXT_HTML_VALUE }, method = RequestMethod.GET)
	public String viewRegister() {
		return "theme1/register";
	}
}
