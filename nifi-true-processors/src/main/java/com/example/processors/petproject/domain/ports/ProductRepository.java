package com.example.processors.petproject.domain.ports;

import com.example.processors.petproject.domain.model.Product;
import org.apache.nifi.controller.ControllerService;

public interface ProductRepository extends ControllerService {

    Product save(Product entity);

}
