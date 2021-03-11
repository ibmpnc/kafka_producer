package com.example.demo.service;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.demo.model.KafkaBankAccount;
import com.example.demo.serde.JsonSerializer;


@Service
public class ProducerService {

	
	@Autowired
	RestTemplate restTemplate	;
	
	@Value("${MONGO_END_POINT_URL}")
    private String MONGO_END_POINT_URL;
	
	@Value("${app.topic.example}")
    private String topic;
	
	
	@Value("${END_POINT_URL}")
    private String END_POINT_URL;
	
	
	
	
	
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;


    public void getAllFromMongodb() {
    	
    	
    	
    	
    	ResponseEntity<KafkaBankAccount[]> response
    	  = restTemplate.getForEntity(MONGO_END_POINT_URL+"/accounts", KafkaBankAccount[].class);
    	
    	System.out.println("lenght for KafkaBankAccount[]="+response.getBody().length);
    	
    	
    	Stream<KafkaBankAccount> accountStream = Arrays.stream(response.getBody());
    	accountStream.forEach(a->sendBankAccounts(a));
    	
    	
  	   restTemplate.getForEntity(MONGO_END_POINT_URL+"/accounts/deleteall", KafkaBankAccount[].class);
  	   
  	 
    	
    	
    }
    
    
    
    
    public void sendBankAccounts(KafkaBankAccount accnt) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        JsonSerializer<KafkaBankAccount> accountSerializer = new JsonSerializer<>();
        props.put("value.serializer", accountSerializer.getClass().getName());
      
        KafkaProducer<String, KafkaBankAccount> producer = new KafkaProducer<>(props);
        
        
        try {
        	
        	Random random = new Random();
           
                
               

           
                ProducerRecord<String, KafkaBankAccount> record = new ProducerRecord<>(topic, ""+random.nextGaussian(), accnt);

                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                });

                
               
                System.out.println("Sent account = "+ accnt.toString());
                
                
                
                
                
               

            
        } finally {
            producer.close();
        }

    }
    
    
    public boolean isEdgeServiceUp() {
    	
    boolean result=false;	
    try {	
    	ResponseEntity<Boolean> response
  	  = restTemplate.getForEntity(END_POINT_URL, Boolean.class);
    
    	result=true;	
    }catch(Exception e)
    {
    	System.out.println("Edge still down....");
    }
    	
    	
    	return result;
    	
    }
    

}
