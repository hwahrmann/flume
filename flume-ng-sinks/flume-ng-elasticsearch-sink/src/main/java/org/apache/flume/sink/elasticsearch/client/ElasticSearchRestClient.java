/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_REST_PORT;

/**
 * Rest ElasticSearch client using the ElasticSearch High-Level Rest Client, 
 * which is compatible with ElasticSearch 6.x and above.
*/
public class  ElasticSearchRestClient implements ElasticSearchClient {

  public static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchTransportClient.class);

  private HttpHost[] serverAddresses;
  private ElasticSearchEventSerializer serializer;
  private RestHighLevelClient client;
  private BulkRequest bulkRequest = null;

  @VisibleForTesting
  HttpHost[] getServerAddresses() {
    return serverAddresses;
  }

  /**
   * Rest client for external cluster
   * 
   * @param hostNames
   * @param serializer
   */
  public ElasticSearchRestClient(String[] hostNames,
      ElasticSearchEventSerializer serializer) {
    configureHostnames(hostNames);
    this.serializer = serializer;
    openClient();
  }

  /**
   * Local client only for testing
   *
   * @param serializer
   */
  public ElasticSearchRestClient(ElasticSearchEventSerializer serializer) {
    this.serializer = serializer;
    openLocalClient();
  }

  private void configureHostnames(String[] hostNames) {
    logger.warn(Arrays.toString(hostNames));
    serverAddresses = new HttpHost[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      String[] httpHost = hostNames[i].trim().split(":");
      String host = httpHost[0].trim();
      int port = httpHost.length > 1 ? Integer.parseInt(httpHost[1].trim())
              : DEFAULT_REST_PORT;
      String transportMethod = httpHost.length == 3 ? httpHost[2].trim()
              : "http";
      serverAddresses[i] = new HttpHost(host, port, transportMethod);
    }
  }
  
  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        logger.error("Error closing client: ", e.getMessage());
      }
    }
    client = null;
  }

  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType) throws Exception {
    
    if (bulkRequest == null) {
      bulkRequest = new BulkRequest();
    }

    bulkRequest.add(new IndexRequest(indexNameBuilder.getIndexName(event), indexType)
        .source(Strings.toString(serializer.getContentBuilder(event)), XContentType.JSON));
  }

  @Override
  public void execute() throws Exception {
    try {
      BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
      if (bulkResponse.hasFailures()) {
        throw new EventDeliveryException(bulkResponse.buildFailureMessage());
      }
    } finally {
      bulkRequest = new BulkRequest();
    }
  }

  /**
   * Open Connection to ElasticSearch Rest Client
   */
  private void openClient() {
    logger.info("Using ElasticSearch hostnames: {} ",
        Arrays.toString(serverAddresses));

    close();
    client = new RestHighLevelClient(RestClient.builder(serverAddresses));
  }

  @Override
  public void configure(Context context) {
  }

  /*
   * FOR TESTING ONLY...
   * 
   * Opens a local discovery node for talking to an ElasticSearch server running
   * in the same JVM
   */
  private void openLocalClient() {
    // The elasticsearch-maven-plugin starts a local ES instance
    // which is running on 127.0.0.1:9200
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    String localhost = "127.0.0.1";
    client = new RestHighLevelClient(RestClient.builder(new HttpHost(localhost, 9200, "http")));
  }
}
