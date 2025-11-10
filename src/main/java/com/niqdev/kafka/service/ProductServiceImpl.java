package com.niqdev.kafka.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.niqdev.kafka.event.ProductCreatedEvent;
import com.niqdev.kafka.model.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {

	private final Logger LOGGER = LoggerFactory.getLogger(getClass());
	
	private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public String createProduct(CreateProductRestModel product) throws Exception {
		
		String productId = Uuid.randomUuid().toString();
		
		// TODO: Persist product
		
		ProductCreatedEvent productCreatedEvent = 
				new ProductCreatedEvent(productId, product.getTitle(), product.getPrice(), product.getQuantity());
		
//		sendMessageAsync(productId, productCreatedEvent);
		
		sendMessageSync(productId, productCreatedEvent);
		
		LOGGER.info("***** Returning product ID");
		
		return productId;
	}
	
	private void sendMessageSync(String productId, ProductCreatedEvent productCreatedEvent) throws Exception {
		
		LOGGER.info("***** Before publishing a ProductCreatedEvent");
		
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent).get();
		
		LOGGER.info("***** Partition: " + result.getRecordMetadata().partition());
		LOGGER.info("***** Topic: " + result.getRecordMetadata().topic());
		LOGGER.info("***** Offset: " + result.getRecordMetadata().offset());
	}

	private void sendMessageAsync(String productId, ProductCreatedEvent productCreatedEvent) {
		CompletableFuture<SendResult<String, ProductCreatedEvent>> furure = 
				kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent);
		
		furure.whenComplete((result, exception) -> {
			if (exception != null) {
				LOGGER.error("***** Failed to send message: " + exception.getMessage());
			} else {
				LOGGER.info("***** Message sent successfully: " + result.getRecordMetadata());
			}
		});
	}

}
