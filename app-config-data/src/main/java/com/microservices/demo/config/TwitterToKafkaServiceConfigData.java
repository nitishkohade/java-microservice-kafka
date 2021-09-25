package com.microservices.demo.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {

	private List<String> twitterKeywords;
	private String welcomeMessage;
	private Boolean enableMockTweets;
	private Integer mockMinTweetLength;
	private Integer mockMaxTweetLength;
	private Integer mockSleepMs;
}
