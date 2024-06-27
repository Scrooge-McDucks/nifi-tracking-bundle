package org.dfm.tracking.processors;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PullResponseItem;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;
import okhttp3.OkHttpClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dfm.tracking.services.InfluxDBControllerService;
import org.dfm.tracking.services.InfluxDBService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class GenerateDataItemIdProcessorTest {

    private TestRunner testRunner;
    private InfluxDBService influxDBService;
    private DockerClient dockerClient;
    private CreateContainerResponse container;

    private static final String INFLUXDB_IMAGE = "influxdb:latest";
    private static final String INFLUXDB_URL = "http://localhost:8086";
    private static final String INFLUXDB_DATABASE = "testdb";
    private static final String INFLUXDB_CONTAINER_NAME = "influxdb-test";
    private static final int INFLUXDB_PORT = 8086;

    @Before
    public void init() throws InterruptedException, IOException, InitializationException {
        // Set up Docker client
        dockerClient = DockerClientBuilder.getInstance().build();

        // Pull the InfluxDB image
        dockerClient.pullImageCmd(INFLUXDB_IMAGE).exec(new PullImageResultCallback() {
            @Override
            public void onNext(PullResponseItem item) {
                System.out.println(item.getStatus());
                super.onNext(item);
            }
        }).awaitCompletion();

        // Create and start the InfluxDB container
        container = dockerClient.createContainerCmd(INFLUXDB_IMAGE)
                .withName(INFLUXDB_CONTAINER_NAME)
                .withPortBindings(new PortBinding(Ports.Binding.bindPort(INFLUXDB_PORT), new ExposedPort(INFLUXDB_PORT)))
                .exec();

        dockerClient.startContainerCmd(container.getId()).exec();

        // Wait for InfluxDB to be ready
        Thread.sleep(5000);

        // Initialize InfluxDB
        Runtime.getRuntime().exec("docker exec " + INFLUXDB_CONTAINER_NAME + " influx -execute 'CREATE DATABASE " + INFLUXDB_DATABASE + "'");

        // Set up NiFi test runner
        influxDBService = mock(InfluxDBControllerService.class);
        testRunner = TestRunners.newTestRunner(GenerateDataflowTrackingId.class);
        testRunner.addControllerService("influxdbService", influxDBService);
        testRunner.setProperty(influxDBService,  InfluxDBControllerService.INFLUXDB_URL, INFLUXDB_URL);
        testRunner.setProperty(influxDBService, InfluxDBControllerService.INFLUXDB_DATABASE, INFLUXDB_DATABASE);
        testRunner.enableControllerService(influxDBService);
        testRunner.setProperty(GenerateDataflowTrackingId.INFLUXDB_SERVICE, influxDBService.getIdentifier());
        testRunner.setProperty(GenerateDataflowTrackingId.NIFI_COMPONENT, "testComponent");

        when(influxDBService.getHttpClient()).thenReturn(new OkHttpClient());
        when(influxDBService.getInfluxdbUrl()).thenReturn(INFLUXDB_URL);
        when(influxDBService.getInfluxdbDatabase()).thenReturn(INFLUXDB_DATABASE);
    }

    @After
    public void tearDown() throws IOException {
        // Stop and remove the InfluxDB container
        if (container != null) {
            dockerClient.stopContainerCmd(container.getId()).exec();
            dockerClient.removeContainerCmd(container.getId()).exec();
        }

        // Close Docker client
        if (dockerClient != null) {
            dockerClient.close();
        }
    }

    @Test
    public void testProcessor() throws InterruptedException {
        // Enqueue a test FlowFile
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Verify the FlowFile was transferred to the success relationship
        testRunner.assertAllFlowFilesTransferred(GenerateDataflowTrackingId.REL_SUCCESS, 1);

        // Verify the FlowFile attributes
        testRunner.getFlowFilesForRelationship(GenerateDataflowTrackingId.REL_SUCCESS).get(0)
                .assertAttributeExists("dataflow.tracking.id");
        testRunner.getFlowFilesForRelationship(GenerateDataflowTrackingId.REL_SUCCESS).get(0)
                .assertAttributeEquals("nifi_component", "testComponent");
    }
}
