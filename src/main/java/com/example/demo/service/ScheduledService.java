package com.example.demo.service;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component("scheduledService")
public class ScheduledService {

	
	@Autowired
	RestTemplate restTemplate	;
	
	
	
	 private KafkaStreams bankAccountStream;
	   
	 
	 @Autowired
	 ProducerService producerService;
	    
	    
	   
	    
	    @Value("${app.topic.example}")
	    private String topic;
	    
	    
	    
	    @Value("${spring.kafka.bootstrap-servers}")
	    private String bootstrapServers;
	
	
	//@Scheduled(fixedDelay = 60000)
    public void scheduleFixedDelayTask() {
        System.out.println("Fixed delay task - " + System.currentTimeMillis() / 1000);
        
        if (this.producerService.isEdgeServiceUp()) {
        	producerService.getAllFromMongodb();
        }
       
        
    }
	
	
	
	
	
    
}
