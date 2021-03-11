package com.example.demo;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.KafkaBankAccount;
import com.example.demo.serde.JsonSerializer;
import com.example.demo.service.ProducerService;

@RestController
public class KafkaProducerController {

    

	@Value("${app.topic.example}")
    private String topic;
	
	@Value("${xmlapp.topic.example}")
    private String xmltopic;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    
    @Autowired
	 ProducerService producerService;

   

    @RequestMapping("/sendBankAccounts/")
    public void sendBankAccounts() {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        JsonSerializer<KafkaBankAccount> accountSerializer = new JsonSerializer<>();
        props.put("value.serializer", accountSerializer.getClass().getName());
      
        KafkaProducer<String, KafkaBankAccount> producer = new KafkaProducer<>(props);
        
        
        try {
        	
        	Random random = new Random();
            while (true) {
                // Every second send a message
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {}

                
                
                KafkaBankAccount accnt = new KafkaBankAccount("jkajfajsl","1000","Wassim","Toronto","Saving");
           
                ProducerRecord<String, KafkaBankAccount> record = new ProducerRecord<>(topic, ""+random.nextGaussian(), accnt);

                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                });

                
               
                System.out.println("Sent account = "+ accnt.toString());
                
                
                
                
                
               

            }
        } finally {
            producer.close();
        }

    }
    
    
  
    @RequestMapping("/getAllBankAccountsFromMongodb/")
    public void getAllBankAccountsFromMongodb() {
    	producerService.getAllFromMongodb();
    }
    
    
    
    
   
    
    
    
    
    
    @RequestMapping("/sendxmlBankAccounts/")
    public void sendxmlBankAccounts() {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
       

        String accnt="";
        try {
        	
        	Random random = new Random();
            while (true) {
                // Every second send a message
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {}

                accnt ="<kafkaXMLBankAccount><balance>1000</balance><name>Wassim</name><address>Toronto</address><accountType>Saving</accountType></kafkaXMLBankAccount>";
                System.out.println("Sent account = "+ accnt.toString());
				ProducerRecord<String, String> record = new ProducerRecord<>(xmltopic, ""+random.nextGaussian(), accnt);

				    producer.send(record, (RecordMetadata r, Exception e) -> {
				        if (e != null) {
				            System.out.println("Error producing events");
				            e.printStackTrace();
				        }
				    });
	   

            }
        } finally {
            producer.close();
        }

    }



}
