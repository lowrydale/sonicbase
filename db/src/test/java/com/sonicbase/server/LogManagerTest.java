package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Indices;
import com.sonicbase.query.DatabaseException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LogManagerTest {

  private DatabaseServer server;
  private DatabaseCommon common;
  private DatabaseClient client;
  private Indices indices;
  private ServersConfig serversConfig;

  @BeforeClass
  public void beforeClass() throws IOException {
    server = mock(DatabaseServer.class);
    Random rand = new Random(System.currentTimeMillis());
    String dataDir = System.getProperty("java.io.tmpdir") + "/" + rand.nextLong() + "/database";

    when(server.getDataDir()).thenReturn(dataDir);

    FileUtils.deleteDirectory(new File(dataDir));

    common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);
    when(server.getReplicationFactor()).thenReturn(2);
    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);

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
    serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);

  }
  @Test
  public void test() throws IOException, InterruptedException {
    Random rand = new Random(System.currentTimeMillis());
    String dataDir = System.getProperty("java.io.tmpdir") + "/database";

    FileUtils.deleteDirectory(new File(dataDir));
    com.sonicbase.server.LogManager logManager = new com.sonicbase.server.LogManager(server, new File(dataDir));

    ComObject cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(SERIALIZATION_VERSION));

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
              ServersConfig sc = new ServersConfig(cobj.getByteArray(ComObject.Tag.SERVERS_CONFIG), SERIALIZATION_VERSION);
              common.setServersConfig(sc);
              return null;
            }
            catch (IOException e) {
              throw new DatabaseException(e);
            }
          }
        });

    logManager.applyLogs();

    //assertFalse(common.getServersConfig().getShards()[0].getReplicas()[0].isDead());

    logManager.shutdown();

    FileUtils.deleteDirectory(new File(dataDir));
  }

  @Test
  public void testPeer() throws IOException, InterruptedException {
    Random rand = new Random(System.currentTimeMillis());
    String dataDir = System.getProperty("java.io.tmpdir") +  "/database";

    FileUtils.deleteDirectory(new File(dataDir));
    LogManager logManager = new LogManager(server, new File(dataDir));

    ComObject cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(SERIALIZATION_VERSION));

    byte[] body = cobj.serialize();

    logManager.logRequestForPeer(body, "DatabaseServer:updateServersConfig", 100, 100, 1);

    Thread.sleep(2_000);
    assertTrue(0 != org.apache.commons.io.FileUtils.sizeOfDirectory(new File(dataDir + "/0/0/peer-1")));
    logManager.shutdown();
    logManager.deletePeerLogs(1);
    assertTrue(0 == org.apache.commons.io.FileUtils.sizeOfDirectory(new File(dataDir + "/0/0/peer-1")));
  }

  @Test
  public void testPeerSliceLogs() throws IOException, InterruptedException {
    String dataDir = System.getProperty("java.io.tmpdir") + "/database";

    FileUtils.deleteDirectory(new File(dataDir));
    LogManager logManager = new LogManager(server, new File(dataDir));

    ComObject cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(SERIALIZATION_VERSION));

    byte[] body = cobj.serialize();

    logManager.logRequestForPeer(body, "DatabaseServer:updateServersConfig", 100, 100, 1);

    Thread.sleep(2_000);

    assertTrue(0 != org.apache.commons.io.FileUtils.sizeOfDirectory(new File(dataDir + "/0/0/peer-1")));

    StringBuilder builder = new StringBuilder();
    logManager.sliceLogsForPeers(true, builder);
    String slice = builder.toString();
    assertFalse(slice.isEmpty());
    assertTrue(slice.endsWith(".bin\n"));

    logManager.shutdown();
  }

  @Test
  public void testSliceLogs() throws IOException, InterruptedException {
    Random rand = new Random(System.currentTimeMillis());
    String dataDir = System.getProperty("java.io.tmpdir") + "/database";

    FileUtils.deleteDirectory(new File(dataDir));
    LogManager logManager = new LogManager(server, new File(dataDir));

    ComObject cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(SERIALIZATION_VERSION));

    byte[] body = cobj.serialize();

    LogManager.LogRequest request = logManager.logRequest(body, true, "DatabaseServer:updateServersConfig",
        null, null, new AtomicLong());
    request.getLatch().await();

    assertTrue(0 != org.apache.commons.io.FileUtils.sizeOfDirectory(new File(dataDir + "/0/0/self")));

    String slice = logManager.sliceLogs(false);
    assertFalse(slice.isEmpty());
    assertTrue(slice.endsWith(".bin\n"));

    logManager.shutdown();

    logManager.deleteLogs();

    assertFalse(new File(dataDir + "/self").exists());

  }

  @Test
  public void testSendLogs() throws IOException {
    String dataDir = System.getProperty("java.io.tmpdir") + "/database";

    FileUtils.deleteDirectory(new File(dataDir));
    LogManager logManager = new LogManager(server, new File(dataDir));

    ComObject cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
    cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(SERIALIZATION_VERSION));

    byte[] body = cobj.serialize();

    logManager.logRequestForPeer(body, "DatabaseServer:updateServersConfig", 100, 100, 1);

    assertTrue(0 != org.apache.commons.io.FileUtils.sizeOfDirectory(new File(dataDir + "/0/0/peer-1")));

    cobj = new ComObject(1);
    cobj.put(ComObject.Tag.REPLICA, 1);
    ComObject retObj = logManager.sendLogsToPeer(cobj, false);
    ComArray array = retObj.getArray(ComObject.Tag.FILENAMES);
    assertTrue(array.getArray().size() != 0);
    assertTrue(((String)array.getArray().get(0)).endsWith(".bin"));

    logManager.shutdown();
  }
}
