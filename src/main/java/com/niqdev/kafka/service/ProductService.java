package com.niqdev.kafka.service;

import com.niqdev.kafka.model.CreateProductRestModel;

public interface ProductService {
	String createProduct(CreateProductRestModel product) throws Exception;
}
