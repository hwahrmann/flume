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

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestElasticSearchRestClient {

  private ElasticSearchRestClient fixture;

  @Mock
  private ElasticSearchEventSerializer serializer;

  @Mock
  private IndexNameBuilder nameBuilder;
  
  @Mock
  private Event event;

  private static final String INDEX_NAME = "foo_index";
  private static final String MESSAGE_CONTENT = "{\"body\":\"test\"}";
  private static final byte[] FAKE_BYTES = new byte[]{9, 8, 7, 6};

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    BytesReference bytesReference = mock(BytesReference.class);
    BytesStream bytesStream = mock(BytesStream.class);
    XContentBuilder builder = jsonBuilder().startObject();
    ContentBuilderUtil.appendField(builder, "@message", FAKE_BYTES);
    builder.endObject();

    when(nameBuilder.getIndexName(any(Event.class))).thenReturn(INDEX_NAME);
    when(bytesReference.toBytesRef()).thenReturn(new BytesRef(MESSAGE_CONTENT));
    when(bytesStream.bytes()).thenReturn(bytesReference);
    when(serializer.getContentBuilder(any(Event.class))).thenReturn(builder);
    fixture = new ElasticSearchRestClient(serializer);
  }

  @Test()
  public void shouldRetryBulkOperation() throws Exception {
    fixture.addEvent(event, nameBuilder, "bar_type");
    fixture.execute();
  }
}
