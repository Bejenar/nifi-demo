package com.example.processors.petproject.infrastructure.controller;

import com.example.processors.petproject.domain.model.Product;
import com.example.processors.petproject.domain.ports.ProductRepository;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.hibernate.HibernateException;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Tags({"hibernate", "jdbc", "database", "connection"})
@CapabilityDescription("Hibernate based Product repository")
public class HibernateProductRepositoryControllerService extends AbstractControllerService implements ProductRepository {

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by your DBMS.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .description("Database driver class name")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID) // to allow empty password for testing
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    private static final List<PropertyDescriptor> properties;

    private EntityManagerFactory entityManagerFactory;

    static {
        properties = List.of(DATABASE_URL, DB_DRIVERNAME, DB_USER, DB_PASSWORD);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.entityManagerFactory = createEntityManagerFactory(context);
    }

    private EntityManagerFactory createEntityManagerFactory(ConfigurationContext context) {
        final String url = getUrl(context);
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();

        var properties = Map.of(
                "javax.persistence.jdbc.driver", driverName,
                "javax.persistence.jdbc.url", url,
                "javax.persistence.jdbc.user", user,
                "javax.persistence.jdbc.password", password);

        return Persistence.createEntityManagerFactory("my-persistence-unit", properties);
    }

    private String getUrl(ConfigurationContext context) {
        return context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Product save(Product entity) {
        var entityManager = entityManagerFactory.createEntityManager(); // TODO this is potentially very bad, use connection pool instead????
        Objects.requireNonNull(entity, "Entity must not be null.");
        try {
            entityManager.getTransaction().begin();
            entityManager.persist(entity);
            return entity;
        } catch (HibernateException e) {
            entityManager.getTransaction().setRollbackOnly();
            throw e;
        } finally {
            entityManager.getTransaction().commit();
        }
    }
}
