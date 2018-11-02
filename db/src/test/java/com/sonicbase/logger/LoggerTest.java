/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class LoggerTest {

  @Test
  public void test() {

    //Logger logger = new Logger(LoggerTest.class);

    //logger.error("error logging id=1, table=persons, db=db, cluster=1-local", new DatabaseException());
  }

  @Test
  public void testParseMessage1() throws IOException {
    String msg = "starting stream producer: config={className=com.sonicbase.streams.ElasticsearchProducer, mappingFile=es-mapping.yaml, maxBatchSize=200}";

    ObjectNode node = new ObjectMapper().createObjectNode();

    Logger.parseMessage(null, msg, node, msg);

    assertEquals(node.get("config").asText(), "{className=com.sonicbase.streams.ElasticsearchProducer, mappingFile=es-mapping.yaml, maxBatchSize=200}");
  }

  @Test
  public void testParseMessage2() throws IOException {
    String msg = "Error initializing stream producer: config=[{producer={className=com.sonicbase.streams.ElasticsearchProducer, mappingFile=es-mapping.yaml, maxBatchSize=200}}]\n";

    ObjectNode node = new ObjectMapper().createObjectNode();

    Logger.parseMessage(null, msg, node, msg);

    assertEquals(node.get("config").asText(), "[{producer={className=com.sonicbase.streams.ElasticsearchProducer, mappingFile=es-mapping.yaml, maxBatchSize=200}}]");
  }
}
