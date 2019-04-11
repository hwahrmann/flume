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
package org.apache.flume.sink.elasticsearch;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.junit.Assert.assertEquals;

public abstract class AbstractElasticSearchSinkTest {

  static final String DEFAULT_INDEX_NAME = "flume";
  static final String DEFAULT_INDEX_TYPE = "log";
  static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
  static final long FIXED_TIME_MILLIS = 123456789L;

  RestHighLevelClient client;
  String timestampedIndexName;
  Map<String, String> parameters;

  void initDefaults() {
    parameters = MapBuilder.<String, String>newMapBuilder().map();
    parameters.put(INDEX_NAME, DEFAULT_INDEX_NAME);
    parameters.put(INDEX_TYPE, DEFAULT_INDEX_TYPE);
    parameters.put(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    parameters.put(BATCH_SIZE, "1");

    timestampedIndexName = DEFAULT_INDEX_NAME + '-'
        + ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS);
  }

  void createClient() throws Exception {
    String hostname = "localhost";
    client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, 9200, "http")));
  }

  void shutdownClient() throws Exception {
    if (client != null) {
      client.close();
    }
  }


  void refreshIndex(String indexName) {
    try {
      client.getLowLevelClient()
          .performRequest("POST", "/" + indexName + "/_refresh");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void deleteIndex(String indexName) {
    try {
      DeleteIndexRequest request = new DeleteIndexRequest(indexName);
      client.indices().delete(request);
    } catch (ElasticsearchException ex) {
      if (ex.status() != RestStatus.NOT_FOUND) {
        // We accept an Index not found, but would like to get an error on everything else
        ex.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setFixedJodaTime() {
    DateTimeUtils.setCurrentMillisFixed(FIXED_TIME_MILLIS);
  }

  @After
  public void resetJodaTime() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  Channel bindAndStartChannel(ElasticSearchSink fixture) {
    // Configure the channel
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    // Wire them together
    fixture.setChannel(channel);
    fixture.start();
    return channel;
  }

  void assertMatchAllQuery(int expectedHits, Event... events) {
    assertSearch(expectedHits, performSearch(QueryBuilders.matchAllQuery()),
        null, events);
  }

  void assertBodyQuery(int expectedHits, Event... events) {
    // Perform Multi Field Match
    assertSearch(expectedHits,
        performSearch(QueryBuilders.matchQuery("message", "event")),
        null, events);
  }

  SearchResponse performSearch(QueryBuilder query) {
    try {
      return client.search(new SearchRequest(timestampedIndexName)
          .source(new SearchSourceBuilder().query(query)));
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  void assertSearch(int expectedHits, SearchResponse response, Map<String, Object> expectedBody,
                    Event... events) {
    SearchHits hitResponse = response.getHits();
    assertEquals(expectedHits, hitResponse.getTotalHits());

    SearchHit[] hits = hitResponse.getHits();
    Arrays.sort(hits, new Comparator<SearchHit>() {
      @Override
      public int compare(SearchHit o1, SearchHit o2) {
        return o1.getSourceAsString().compareTo(o2.getSourceAsString());
      }
    });

    for (int i = 0; i < events.length; i++) {
      Event event = events[i];
      SearchHit hit = hits[i];
      Map<String, Object> source = hit.getSourceAsMap();
      if (expectedBody == null) {
        assertEquals(new String(event.getBody()), source.get("message"));
      } else {
        assertEquals(expectedBody, source.get("message"));
      }
    }
  }

}
