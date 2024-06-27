package org.dfm.tracking.processors;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dfm.tracking.services.InfluxDBService;

import java.io.IOException;
import java.util.*;

@EventDriven
@SideEffectFree
@Tags({"example", "data item", "identifier"})
@CapabilityDescription("Generate a unique data item ID when a flow file enters the DDS and include the NiFi component name.")
public class GenerateDataflowTrackingId extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful flow files are routed here")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor INFLUXDB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDB Service")
            .description("InfluxDB Controller Service")
            .required(true)
            .identifiesControllerService(InfluxDBService.class)
            .build();

    public static final PropertyDescriptor NIFI_COMPONENT = new PropertyDescriptor.Builder()
            .name("NiFi Component")
            .description("Name of the NiFi component where the flow file enters the DDS")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INFLUXDB_SERVICE);
        descriptors.add(NIFI_COMPONENT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Generate a unique data item ID
        String dataItemId = UUID.randomUUID().toString();

        // Get InfluxDB service
        InfluxDBService influxDBService = context.getProperty(INFLUXDB_SERVICE).asControllerService(InfluxDBService.class);

        // Get InfluxDB properties from the service
        String influxdbUrl = influxDBService.getInfluxdbUrl();
        String influxdbDatabase = influxDBService.getInfluxdbDatabase();
        OkHttpClient httpClient = influxDBService.getHttpClient();
        String nifiComponent = context.getProperty(NIFI_COMPONENT).getValue();

        // Add the data item ID as an attribute
        flowFile = session.putAttribute(flowFile, "dataflow.tracking.id", dataItemId);
        flowFile = session.putAttribute(flowFile, "nifi_component", nifiComponent);

        // Create a point to write to InfluxDB
        String influxData = String.format("%s,dataflow.tracking.id=%s,nifi_component=%s event_type=\"enter_dds\"",
                influxdbDatabase, dataItemId, nifiComponent);

        // Write the point to InfluxDB
        RequestBody body = RequestBody.create(influxData, okhttp3.MediaType.parse("text/plain"));
        Request.Builder requestBuilder = new Request.Builder()
                .url(influxdbUrl + "/write?db=" + influxdbDatabase)
                .post(body);

        // Add authentication if necessary
        requestBuilder = influxDBService.addAuthentication(requestBuilder);

        Request request = requestBuilder.build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
        } catch (IOException e) {
            getLogger().error("Failed to write to InfluxDB", e);
            session.transfer(flowFile, Relationship.SELF);
            return;
        }

        // Transfer the flow file to the success relationship
        session.transfer(flowFile, REL_SUCCESS);
    }
}
