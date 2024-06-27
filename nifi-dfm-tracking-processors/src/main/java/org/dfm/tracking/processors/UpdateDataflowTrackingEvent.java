package org.dfm.tracking.processors;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
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
@Tags({"example"})
@CapabilityDescription("Update event when a data item leaves a NiFi component, enters a new NiFi component, or leaves the DDS with system name.")
public class UpdateDataflowTrackingEvent extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful flow files are routed here")
            .build();

    public static final PropertyDescriptor INFLUXDB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDB Service")
            .description("InfluxDB Controller Service")
            .required(true)
            .identifiesControllerService(InfluxDBService.class)
            .build();

    public static final AllowableValue EVENT_ENTER_DDS = new AllowableValue("enter_dds", "Enter DDS", "The data item enters the DDS.");
    public static final AllowableValue EVENT_LEAVE_DDS = new AllowableValue("leave_dds", "Leave DDS", "The data item leaves the DDS.");
    public static final AllowableValue EVENT_ENTER_NIFI = new AllowableValue("enter_nifi", "Enter NiFi", "The data item enters a NiFi component.");
    public static final AllowableValue EVENT_LEAVE_NIFI = new AllowableValue("leave_nifi", "Leave NiFi", "The data item leaves a NiFi component.");

    public static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("Event Type")
            .description("Type of event")
            .required(true)
            .allowableValues(EVENT_ENTER_DDS, EVENT_LEAVE_DDS, EVENT_ENTER_NIFI, EVENT_LEAVE_NIFI)
            .defaultValue(EVENT_ENTER_DDS.getValue())
            .build();

    public static final PropertyDescriptor NIFI_COMPONENT = new PropertyDescriptor.Builder()
            .name("NiFi Component")
            .description("Name of the NiFi component")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SYSTEM_NAME = new PropertyDescriptor.Builder()
            .name("System Name")
            .description("Name of the system to which the data item is sent (only for leave_dds)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INFLUXDB_SERVICE);
        descriptors.add(EVENT_TYPE);
        descriptors.add(NIFI_COMPONENT);
        descriptors.add(SYSTEM_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);
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

        // Get necessary attributes and properties
        String dataItemId = flowFile.getAttribute("dataflow.tracking.id");
        String eventType = context.getProperty(EVENT_TYPE).getValue();
        InfluxDBService influxDBService = context.getProperty(INFLUXDB_SERVICE).asControllerService(InfluxDBService.class);
        String influxdbUrl = influxDBService.getInfluxdbUrl();
        String influxdbDatabase = influxDBService.getInfluxdbDatabase();
        OkHttpClient httpClient = influxDBService.getHttpClient();
        String nifiComponent = context.getProperty(NIFI_COMPONENT).getValue();
        String systemName = context.getProperty(SYSTEM_NAME).getValue();

        // Create a point to write to InfluxDB
        String influxData = influxdbDatabase + ",dataflow.tracking.id=" + dataItemId + " event_type=\"" + eventType + "\"";
        if (nifiComponent != null && !nifiComponent.isEmpty()) {
            influxData += ",nifi_component=\"" + nifiComponent + "\"";
        }
        if (eventType.equals("leave_dds") && systemName != null) {
            influxData += ",system=\"" + systemName + "\"";
        }

        // Write the point to InfluxDB
        RequestBody body = RequestBody.create(influxData, okhttp3.MediaType.parse("text/plain"));
        Request request = new Request.Builder()
                .url(influxdbUrl + "/write?db=" + influxdbDatabase)
                .post(body)
                .build();

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
