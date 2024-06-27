package org.dfm.tracking.services;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBControllerService extends AbstractControllerService implements InfluxDBService {

    public static final PropertyDescriptor INFLUXDB_URL = new PropertyDescriptor.Builder()
            .name("InfluxDB URL")
            .description("URL of the InfluxDB instance")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUXDB_DATABASE = new PropertyDescriptor.Builder()
            .name("InfluxDB Database")
            .description("Database name in InfluxDB")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUXDB_USERNAME = new PropertyDescriptor.Builder()
            .name("InfluxDB Username")
            .description("Username for InfluxDB authentication")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUXDB_PASSWORD = new PropertyDescriptor.Builder()
            .name("InfluxDB Password")
            .description("Password for InfluxDB authentication")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private OkHttpClient httpClient;
    private String influxdbUrl;
    private String influxdbDatabase;
    private String influxdbUsername;
    private String influxdbPassword;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INFLUXDB_URL);
        descriptors.add(INFLUXDB_DATABASE);
        descriptors.add(INFLUXDB_USERNAME);
        descriptors.add(INFLUXDB_PASSWORD);
        return descriptors;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.influxdbUrl = context.getProperty(INFLUXDB_URL).getValue();
        this.influxdbDatabase = context.getProperty(INFLUXDB_DATABASE).getValue();
        this.influxdbUsername = context.getProperty(INFLUXDB_USERNAME).getValue();
        this.influxdbPassword = context.getProperty(INFLUXDB_PASSWORD).getValue();
        this.httpClient = new OkHttpClient();
    }

    @OnDisabled
    public void onDisabled() {
        this.httpClient = null;
        this.influxdbUrl = null;
        this.influxdbDatabase = null;
        this.influxdbUsername = null;
        this.influxdbPassword = null;
    }

    @Override
    public OkHttpClient getHttpClient() {
        return this.httpClient;
    }

    @Override
    public String getInfluxdbUrl() {
        return this.influxdbUrl;
    }

    @Override
    public String getInfluxdbDatabase() {
        return this.influxdbDatabase;
    }

    @Override
    public String getInfluxdbUsername() {
        return this.influxdbUsername;
    }

    @Override
    public String getInfluxdbPassword() {
        return this.influxdbPassword;
    }

    public Request.Builder addAuthentication(Request.Builder requestBuilder) {
        if (influxdbUsername != null && influxdbPassword != null) {
            String credential = Credentials.basic(influxdbUsername, influxdbPassword);
            requestBuilder.addHeader("Authorization", credential);
        }
        return requestBuilder;
    }
}
