package com.sonicbase.index;

import com.sonicbase.server.DatabaseServer;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class AddressMapTest {

  @Test
  public void testToUnsafe() {
    DatabaseServer server = mock(DatabaseServer.class);
    when(server.useUnsafe()).thenReturn(false);
    //when(server.compressRecords()).thenReturn(true);
    AddressMap map = new AddressMap(server);
    byte[][] records = new byte[][]{new byte[]{1}, new byte[]{2}};
    Object obj = map.toUnsafeFromRecords(1, records);
    byte[][] after = map.fromUnsafeToRecords(obj);
    assertEquals(records[0], after[0]);
    assertEquals(records[1], after[1]);
  }

  @Test
  public void testToUnsafe2() {
    DatabaseServer server = mock(DatabaseServer.class);
    when(server.useUnsafe()).thenReturn(true);
    //when(server.compressRecords()).thenReturn(true);
    AddressMap map = new AddressMap(server);
    byte[][] records = new byte[][]{new byte[]{1}, new byte[]{2}};
    Object obj = map.toUnsafeFromRecords(1, records);
    byte[][] after = map.fromUnsafeToRecords(obj);
    assertEquals(records[0], after[0]);
    assertEquals(records[1], after[1]);
  }

  @Test
  public void testToUnsafe2Compressed() {
    DatabaseServer server = mock(DatabaseServer.class);
    when(server.useUnsafe()).thenReturn(true);
    when(server.compressRecords()).thenReturn(true);
    AddressMap map = new AddressMap(server);
    byte[][] records = new byte[][]{new byte[]{1}, new byte[]{2}};
    Object obj = map.toUnsafeFromRecords(1, records);
    byte[][] after = map.fromUnsafeToRecords(obj);
    assertEquals(records[0], after[0]);
    assertEquals(records[1], after[1]);
  }
}
