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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Serialize flume events into the same format LogStash uses</p>
 *
 * This can be used to send events to ElasticSearch and use clients such as
 * Kibana which expect Logstash formated indexes
 *
 * <pre>
 * {
 *    "@timestamp": "2019-04-11T21:48:33.309258Z",
 *    "@source": "source of the event, usually a URL."
 *     "message": "the original plain-text message"
 *     # any further fields, that are available in the event, like the following:
 *     "user" : "a user name in the event"
 *     "ip_src": "the source ip in the event"
 *   }
 * </pre>
 *
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 *
 * <pre>
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 */
public class ElasticSearchLogStashEventSerializer implements
    ElasticSearchEventSerializer {

  @Override
  public XContentBuilder getContentBuilder(Event event) throws IOException {
    XContentBuilder builder = jsonBuilder().startObject();
    appendFields(builder, event);
    builder.endObject();
    return builder;
  }

  private void appendFields(XContentBuilder builder, Event event)
      throws IOException {
    
    // Append the Event Body
    byte[] body = event.getBody();
    ContentBuilderUtil.appendField(builder, "message", body);
    
    // And now append the Event Header Fields
    Map<String, String> headers = MapBuilder.<String, String>newMapBuilder()
        .putAll(event.getHeaders()).map();

    String timestamp = headers.get("timestamp");
    if (!StringUtils.isBlank(timestamp)
        && StringUtils.isBlank(headers.get("@timestamp"))) {
      long timestampMs = Long.parseLong(timestamp);
      builder.field("@timestamp", new Date(timestampMs));
    }

    String source = headers.get("source");
    if (!StringUtils.isBlank(source)
        && StringUtils.isBlank(headers.get("@source"))) {
      ContentBuilderUtil.appendField(builder, "@source",
          source.getBytes(charset));
    }

    String type = headers.get("type");
    if (!StringUtils.isBlank(type)
        && StringUtils.isBlank(headers.get("@type"))) {
      ContentBuilderUtil.appendField(builder, "@type", type.getBytes(charset));
    }

    String host = headers.get("host");
    if (!StringUtils.isBlank(host)
        && StringUtils.isBlank(headers.get("@source_host"))) {
      ContentBuilderUtil.appendField(builder, "@source_host",
          host.getBytes(charset));
    }

    String srcPath = headers.get("src_path");
    if (!StringUtils.isBlank(srcPath)
        && StringUtils.isBlank(headers.get("@source_path"))) {
      ContentBuilderUtil.appendField(builder, "@source_path",
          srcPath.getBytes(charset));
    }

    for (String key : headers.keySet()) {
      byte[] val = headers.get(key).getBytes(charset);
      ContentBuilderUtil.appendField(builder, key, val);
    }
  }

  @Override
  public void configure(Context context) {
    // NO-OP...
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }
}
