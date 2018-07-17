/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.FileUtils;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.Indices;
import com.sonicbase.query.DatabaseException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

public class LogManagerTest {

  @Test
  public void test() throws IOException, InterruptedException {

    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    when(server.getDataDir()).thenReturn("/tmp/database");

    FileUtils.deleteDirectory(new File("/tmp/database"));

    final DatabaseCommon common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    Indices indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);

    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
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

    com.sonicbase.server.LogManager logManager = new com.sonicbase.server.LogManager(server, new File("/tmp/database"));

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.method, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.serversConfig, common.getServersConfig().serialize(SERIALIZATION_VERSION));

    byte[] body = cobj.serialize();

    LogManager.LogRequest request = logManager.logRequest(body, true, "DatabaseServer:updateServersConfig",
        null, null, new AtomicLong());

    request.getLatch().await();

    serversConfig.getShards()[0].getReplicas()[0].setDead(true);

    when(server.invokeMethod(any(byte[].class), anyLong(), anyLong(), eq(true), eq(false),
        any(AtomicLong.class), any(AtomicLong.class))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            byte[] bytes = (byte[]) args[0];
            try {
              ComObject cobj = new ComObject(bytes);
              ServersConfig sc = new ServersConfig(cobj.getByteArray(ComObject.Tag.serversConfig), SERIALIZATION_VERSION);
              common.setServersConfig(sc);
              return null;
            }
            catch (IOException e) {
              throw new DatabaseException(e);
            }
          }
        });

    logManager.applyLogs();

    assertFalse(common.getServersConfig().getShards()[0].getReplicas()[0].isDead());
  }
}
