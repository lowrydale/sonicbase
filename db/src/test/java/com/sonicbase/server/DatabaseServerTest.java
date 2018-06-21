/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class DatabaseServerTest {

  @Test
  public void test() throws InterruptedException {

    com.sonicbase.server.DatabaseServer server = new DatabaseServer();
    AtomicBoolean isHealthy = new AtomicBoolean();
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    when(client.send(eq("DatabaseServer:healthCheck"), anyInt(), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.specified))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject retObj = (ComObject)args[3];
            retObj.put(ComObject.Tag.status, "{\"status\" : \"ok\"}");
            return retObj.serialize();
          }
        });


    server.checkHealthOfServer(0, 0, isHealthy, false);

    assertTrue(isHealthy.get());
  }
}
