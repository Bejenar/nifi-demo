package com.example.processors.petproject.infrastructure.processor;

import com.example.processors.petproject.domain.model.Product;
import com.example.processors.petproject.domain.ports.ProductRepository;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.example.processors.petproject.infrastructure.processor.MyProcessor.BATCH_ID;
import static com.example.processors.petproject.infrastructure.processor.MyProcessor.FILE_ID;


@Tags({"example"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Receives json that represents Product entity to further insert it into DB")
@ReadsAttributes({@ReadsAttribute(attribute = FILE_ID, description = "Custom attribute for demo purposes"),
        @ReadsAttribute(attribute = BATCH_ID, description = "Custom attribute for demo purposes")})
public class MyProcessor extends AbstractProcessor {

    public static final String FILE_ID = "FILE_ID";
    public static final String BATCH_ID = "BATCH_ID";

    public static final PropertyDescriptor PRODUCT_REPOSITORY = new PropertyDescriptor.Builder()
            .name("Product repository")
            .description("Repository to work with Product entities")
            .identifiesControllerService(ProductRepository.class)
            .required(true)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("FlowFile that was inserted into DB")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("FlowFile that was not inserted into DB")
            .build();

    private static final List<PropertyDescriptor> descriptors;

    private static final Set<Relationship> relationships;

    private static final ObjectMapper objectMapper;

    static {
        descriptors = List.of(PRODUCT_REPOSITORY);
        relationships = Set.of(SUCCESS, FAILURE);
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            String productJson = fileToString(flowFile, session);
            Product product = objectMapper.readValue(productJson, Product.class);
            String fileId = flowFile.getAttribute(FILE_ID);
            String batchId = flowFile.getAttribute(BATCH_ID);

            product.setFileId(fileId);
            product.setBatchId(batchId);

            ProductRepository productRepository = getProductRepository(context);
            productRepository.save(product);
            session.transfer(flowFile, SUCCESS); // TODO if exception happens after repository saves the product to DB, DB transaction will not be rolled back
        } catch (Exception e) {
            session.transfer(flowFile, FAILURE);
            getLogger().error("Exception: ", e);
        }
    }

    private ProductRepository getProductRepository(final ProcessContext context) {
        return context.getProperty(PRODUCT_REPOSITORY).asControllerService(ProductRepository.class);
    }

    private String fileToString(final FlowFile flowFile, final ProcessSession session) throws IOException {
        InputStream inputStream = session.read(flowFile);
        String productJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        inputStream.close();

        return productJson;
    }
}
