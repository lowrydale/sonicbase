/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ServersConfig;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class DatabaseClientTest {

  @Test
  public void testCreateDatabase() throws IOException {
    DatabaseCommon common = new DatabaseCommon();
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    common.setServersConfig(serversConfig);

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public byte[] sendToMaster(ComObject cobj) {
        assertEquals(cobj.getString(ComObject.Tag.dbName), "test");
        assertEquals(cobj.getString(ComObject.Tag.masterSlave), "master");
        assertEquals(cobj.getString(ComObject.Tag.method), "SchemaManager:createDatabase");
        return null;
      }
      protected void syncConfig() {

      }
    };

    client.createDatabase("test");
  }
}
