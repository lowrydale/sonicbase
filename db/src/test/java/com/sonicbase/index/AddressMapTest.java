package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.server.DatabaseServer;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class AddressMapTest {


  @Test(enabled=false)
  public void testCompact() {
    DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    AddressMap.MAX_PAGE_SIZE = 2048;
    addressMap.stopCompactor();
    MemoryOps memoryOps = new MemoryOps(server, false);
    for (int i = 0; i < 100; i++) {
      MemoryOps.MemoryOp memOp = new MemoryOps.MemoryOp();
      memOp.setRecords(new byte[1][256]);
      memoryOps.add(memOp);
    }
    memoryOps.execute();

    ConcurrentHashMap<Long, AddressMap.Page> pages = addressMap.getPages();

    assertEquals(pages.size(), 13);

    int pageOffset = 0;
    for (AddressMap.Page page : pages.values()) {
      if (pageOffset > 0) {
        long address = page.getBaseAddress();
        address += 8;
        int count = DataUtils.addressToInt(address, addressMap.getUnsafe());
        address += 4;
        long offset = address;
        for (int i = 0; i < count / 2; i++) {
          offset += 1; //single allocation
          offset += 1; //freed
          long outerAddress = DataUtils.addressToLong(offset, AddressMap.getUnsafe());
          offset += 8; //outerAddress
          offset += 4; //offset from top of page allocation
          offset += 8; //update time
          offset += DataUtils.addressToInt(offset, AddressMap.getUnsafe());
          offset += 4; //actualSize

          addressMap.freeUnsafeIds(outerAddress);
        }
      }
      pageOffset++;
    }

    addressMap.doCompact();

    assertEquals(7, pages.size());

    for (AddressMap.Page page : pages.values()) {
      assertEquals(0, page.getTotalFreeSize());
    }
  }



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
