package org.dfm.tracking;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dfm.tracking.services.InfluxDBControllerService;
import org.dfm.tracking.services.InfluxDBService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class GenerateDataflowTrackingTest {

    private TestRunner testRunner;
    private InfluxDBService influxDBService;
    private InfluxDBControllerService influxDBControllerService;
    private OkHttpClient httpClient;
    private Response response;
    private Call call;

    @Before
    public void setUp() throws InitializationException, IOException {
        // Mock the InfluxDBService interface
        influxDBService = Mockito.mock(InfluxDBService.class);
        httpClient = Mockito.mock(OkHttpClient.class);
        response = Mockito.mock(Response.class);
        call = Mockito.mock(Call.class);

        // Mock the methods in InfluxDBService
        when(influxDBService.getHttpClient()).thenReturn(httpClient);
        when(influxDBService.getInfluxdbUrl()).thenReturn("http://localhost:8086");
        when(influxDBService.getInfluxdbDatabase()).thenReturn("testdb");
        when(influxDBService.addAuthentication(any(Request.Builder.class))).thenAnswer(invocation -> {
            Request.Builder builder = invocation.getArgument(0);
            return builder.url("http://localhost:8086/write?db=testdb");
        });

        // Ensure the correct call mock is returned
        when(httpClient.newCall(any(Request.class))).thenReturn(call);
        when(call.execute()).thenReturn(response);

        // Initialize the TestRunner
        testRunner = TestRunners.newTestRunner(GenerateDataflowTrackingId.class);

        // Create and configure the real instance of the controller service
        influxDBControllerService = new InfluxDBControllerService() {
            @Override
            public OkHttpClient getHttpClient() {
                return influxDBService.getHttpClient();
            }

            @Override
            public String getInfluxdbUrl() {
                return influxDBService.getInfluxdbUrl();
            }

            @Override
            public String getInfluxdbDatabase() {
                return influxDBService.getInfluxdbDatabase();
            }

            @Override
            public Request.Builder addAuthentication(Request.Builder builder) {
                return influxDBService.addAuthentication(builder);
            }
        };

        // Add and enable the controller service
        testRunner.addControllerService("influxDBControllerService", influxDBControllerService);
        testRunner.setProperty(influxDBControllerService, InfluxDBControllerService.INFLUXDB_URL, "http://localhost:8086");
        testRunner.setProperty(influxDBControllerService, InfluxDBControllerService.INFLUXDB_DATABASE, "testdb");
        testRunner.enableControllerService(influxDBControllerService);

        // Set the properties for the processor
        testRunner.setProperty(GenerateDataflowTrackingId.INFLUXDB_SERVICE, "influxDBControllerService");
        testRunner.setProperty(GenerateDataflowTrackingId.NIFI_CLUSTER_NAME, "test-cluster");
        testRunner.setProperty(GenerateDataflowTrackingId.MEASUREMENT_NAME, "test_measurement");
    }

    @Test
    public void testOnTrigger_Success() throws IOException {
        when(response.isSuccessful()).thenReturn(true);

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GenerateDataflowTrackingId.REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(GenerateDataflowTrackingId.REL_SUCCESS).get(0).assertAttributeExists("dataflow.tracking.id");
        testRunner.getFlowFilesForRelationship(GenerateDataflowTrackingId.REL_SUCCESS).get(0).assertAttributeEquals("nifi_cluster_name", "test-cluster");

        verify(httpClient, times(1)).newCall(any(Request.class));
    }

    @Test
    public void testOnTrigger_Failure() throws IOException {
        when(response.isSuccessful()).thenReturn(false);

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GenerateDataflowTrackingId.REL_FAILURE, 1);

        verify(httpClient, times(1)).newCall(any(Request.class));
    }

    @Test
    public void testOnTrigger_NullFlowFile() {
        testRunner.run();

        testRunner.assertTransferCount(GenerateDataflowTrackingId.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GenerateDataflowTrackingId.REL_FAILURE, 0);
    }
}
