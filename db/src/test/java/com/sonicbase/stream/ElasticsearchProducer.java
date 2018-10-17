/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.Config;
import com.sonicbase.common.Record;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.UpdateManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class ElasticsearchProducer implements StreamsProducer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ElasticsearchProducer.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  @Override
  public void init(String cluster, Config config, Map<String, Object> streamConfig) {
    try {
      logger.info("Elasticsearch producer init - begin");



      logger.info("Elasticsearch producer init - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void publish(List<String> messages) {
    try {
      StringBuilder builder = new StringBuilder();
      for (String message : messages) {
        JsonNode node = OBJECT_MAPPER.readTree(message);
        JsonNode events = node.withArray("events");
        for (int i = 0; i < events.size(); i++) {
          JsonNode record = events.get(i);
          if ("UPDATE".equals(record.get("_sonicbase_action").asText())) {
            builder.append("\"before\" : {");

            TableSchema tableSchema = server.getCommon().getTableSchema(currRequest.dbName, currRequest.tableName, server.getDataDir());
            Record record = new Record(currRequest.dbName, server.getCommon(), currRequest.existingBytes);
            getJsonFromRecord(builder, tableSchema, record);
            builder.append("},");
            builder.append("\"after\": {");
            record = new Record(currRequest.dbName, server.getCommon(), currRequest.recordBytes);
            getJsonFromRecord(builder, tableSchema, record);
            builder.append("}");

          }
          else if ("INSERT".equals(record.get("_sonicbase_action").asText())) {
            JsonNode esNode = OBJECT_MAPPER.createObjectNode();
            ObjectNode idField = ((ObjectNode) esNode).putObject("index");
            idField.put("_id", id);
            builder.append(esNode.toString());

            esNode = OBJECT_MAPPER.createArrayNode();

            Iterator<Map.Entry<String, JsonNode>> iterator = record.fields();
            while (iterator.hasNext()) {
              Map.Entry<String, JsonNode> field = iterator.next();
              if (field.getKey().startsWith("_sonicbase")) {
                continue;
              }
              ((ObjectNode) esNode).put(field.getKey(), field.getValue());
            }
            builder.append(esNode.toString());
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {
  }
}
