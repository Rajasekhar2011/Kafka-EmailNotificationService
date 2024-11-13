package com.learning.sergey.kafka.EmailNotification.handler;

import com.learning.sergey.kafka.EmailNotification.exception.NotRetryableException;
import com.learning.sergey.kafka.EmailNotification.exception.RetryableException;
import com.learning.sergey.kafka.core.ProductCreatedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventsHandler {

    @Autowired
    RestTemplate restTemplate;

    @KafkaHandler
    public void handler(ProductCreatedEvent productCreatedEvent){
    	 System.out.println("Message arrived for topic " + " product-created-events-topic" + "for event " + "ProductCreatedEvent and key is " + productCreatedEvent.getProductId());
       /* if(true){
            throw  new NotRetryableException("An error took place. No need to consume this message again");
        }else {
            System.out.println("Message arrived for topic " + " product-created-events-topic" + "for event " + "ProductCreatedEvent and key is " + productCreatedEvent.getProductId());
        }*/
        try {
            ResponseEntity<String> response = restTemplate.exchange("http://localhost:9098/response/200", HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                System.out.println("received response from remote service");
            }

        }catch (ResourceAccessException exc){
            System.out.println("Error occured while calling external service "+exc.getMessage());
            throw  new RetryableException(exc);
        }catch (HttpServerErrorException exc){
            System.out.println("Error occured while calling external service "+exc.getMessage());
            throw  new NotRetryableException(exc);
        }catch (Exception exc){
            System.out.println(exc.getMessage());
            throw new NotRetryableException(exc);
        }

    }
}
