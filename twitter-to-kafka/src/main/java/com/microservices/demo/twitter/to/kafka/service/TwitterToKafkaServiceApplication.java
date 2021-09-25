package com.microservices.demo.twitter.to.kafka.service;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
//@Scope bean scope ...singleton, prototype, request, session
//@postconstruct vs ApplicationListener vs CommandLineRunner vs @EventListener
public class TwitterToKafkaServiceApplication implements CommandLineRunner{
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
	
	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	
	private final StreamRunner streamRunner;
	
	public TwitterToKafkaServiceApplication(
			TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
			StreamRunner streamRunner
			) {
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
		this.streamRunner = streamRunner;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}
	
	@PostConstruct
	public void init() {
		System.out.println("postconstruct: twitter to kafka service running");
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("App starts...");
		LOG.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {})));
		LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
		streamRunner.start();
	}

//	@Override
//	public void onApplicationEvent(ApplicationEvent event) {
//				
//	}
	

}
