/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.processors.petproject;

import com.example.processors.petproject.domain.model.Product;
import com.example.processors.petproject.infrastructure.controller.HibernateProductRepositoryControllerService;
import com.example.processors.petproject.infrastructure.processor.MyProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.example.processors.petproject.infrastructure.processor.MyProcessor.PRODUCT_REPOSITORY;


public class MyProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public void testProcessor() throws InitializationException, JsonProcessingException {
        ControllerService service = new HibernateProductRepositoryControllerService();
        testRunner.addControllerService("HibernateProductRepositoryControllerService", service, Map.of(
                "Database Connection URL", "jdbc:mysql://localhost:3306/nifi_demo",
                "Database Driver Class Name", "com.mysql.jdbc.Driver",
                "Database User", "root",
                "Password", ""
        ));
        testRunner.enableControllerService(service);
        testRunner.setProperty(PRODUCT_REPOSITORY, "HibernateProductRepositoryControllerService");

        Product p = new Product();
        p.setName("P22");
        p.setId(123321);

        String data = new ObjectMapper().writeValueAsString(p);
        System.out.println(data);
        testRunner.enqueue(data);
        testRunner.run();
    }

}
