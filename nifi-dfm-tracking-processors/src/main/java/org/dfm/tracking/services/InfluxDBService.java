package org.dfm.tracking.services;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.nifi.controller.ControllerService;

public interface InfluxDBService extends ControllerService {

    OkHttpClient getHttpClient();

    String getInfluxdbUrl();

    String getInfluxdbDatabase();

    String getInfluxdbUsername();

    String getInfluxdbPassword();

    Request.Builder addAuthentication(Request.Builder requestBuilder);
}
