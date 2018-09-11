/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.sonicbase.index.AddressMap.MAX_PAGE_SIZE;
import static com.sonicbase.index.AddressMap.MEM_OP;

public class MemoryOps {
  private final boolean isExplicitTrans;
  private final DatabaseServer server;
  public int phaseCount = MEM_OP ? 2 : 1;
  public ConcurrentHashMap<String, ConcurrentHashMap<Object[], byte[][]>> indices = new ConcurrentHashMap<>();
  private ConcurrentLinkedQueue<MemoryOp> ops = new ConcurrentLinkedQueue<>();
  private int phase = 0;

  public MemoryOps(DatabaseServer server, boolean isExplicitTrans) {
    this.server = server;
    this.isExplicitTrans = isExplicitTrans;
    if (isExplicitTrans) {
      phaseCount = 1;
    }
  }

  public int getPhase() {
    return phase;
  }

  public ConcurrentLinkedQueue<MemoryOp> getOps() {
    return ops;
  }

  public boolean isExplicitTrans() {
    return isExplicitTrans;
  }

  public static class MemoryOp {
    public enum OpType {
      ALLOCATE,
      FREE
    }
    private byte[][] records;
    private Long address;

    public byte[][] getRecords() {
      return records;
    }

    public Long getAddress() {
      return address;
    }

    public void setRecords(byte[][] records) {
      this.records = records;
    }

    public void setAddress(Long address) {
      this.address = address;
    }
  }

  public void add(MemoryOp memOp) {
    ops.add(memOp);
  }

  public void execute() {
    if (phase == 1) {
      if (ops.size() != 0) {
        throw new DatabaseException("not empty");
      }
      return;
    }

    ConcurrentLinkedQueue<MemoryOp> allOps = new ConcurrentLinkedQueue<>();
    if (ops.size() == 1) {
      MemoryOp memOp = ops.poll();
      allOps.add(memOp);
      int recordsLen = 0;
      for (byte[] record : memOp.records) {
        recordsLen += record.length;
      }
      int totalSize = 1 + 1 + 8 + 4 + 8 + 4 + 4 + (4 * memOp.records.length) + recordsLen;

      int actualLen = 4 + (4 * memOp.records.length) + recordsLen;
      int offset = 0;
      byte[] bytes = new byte[totalSize];
      bytes[offset] = 1; //single allocation
      offset += 1;
      bytes[offset] = 0; //freed
      offset += 1;
      offset += 8; //outer address
      offset += 4; // offset from top of page allocation
      offset += 8; // update time
      DataUtils.intToBytes(actualLen, bytes, offset); //actual size
      offset += 4;

      DataUtils.intToBytes(memOp.records.length, bytes, offset);
      offset += 4;
      for (byte[] record : memOp.records) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }

      Unsafe unsafe = AddressMap.getUnsafe();
      long address = unsafe.allocateMemory(totalSize);

      for (int j = 0; j < bytes.length; j++) {
        unsafe.putByte(address + j, bytes[j]);
      }
      memOp.address = server.getAddressMap().addAddress(address);
    }
    else {
      List<List<MemoryOp>> opsLists = new ArrayList<>();
      List<Integer> allocateCounts = new ArrayList<>();
      List<Integer> totalSizes = new ArrayList<>();
      outer:
      while (true) {
        int totalSize = 0;
        totalSize += 8; //page id
        totalSize += 4; //allocation count
        List<MemoryOp> currOps = new ArrayList<>();
        int allocateCount = 0;
        while (true) {
          MemoryOp memOp = ops.poll();
          if (memOp == null) {
            if (!currOps.isEmpty()) {
              opsLists.add(currOps);
              allocateCounts.add(allocateCount);
              totalSizes.add(totalSize);
            }
            break outer;
          }
          allOps.add(memOp);
          int recordsLen = 0;
          for (byte[] record : memOp.records) {
            recordsLen += record.length;
          }
          totalSize += 1 + 1 + 8 + 4 + 8 + 4 + 4 + (4 * memOp.records.length) + recordsLen;
          allocateCount++;
          currOps.add(memOp);
          //        if (totalSize > MAX_PAGE_SIZE) {
          //          opsLists.add(currOps);
          //          allocateCounts.add(allocateCount);
          //          totalSizes.add(totalSize);
          //          break;
          //        }
        }
      }

      for (int i = 0; i < opsLists.size(); i++) {
        int allocateCount = allocateCounts.get(i);
        int totalSize = totalSizes.get(i);
        List<MemoryOp> ops = opsLists.get(i);

        int[] offsets = new int[allocateCount];
        byte[] bytes = new byte[totalSize];
        int offset = 8; //page id
        offset += 4; //allocation count
        int currOffset = 0;
        for (MemoryOp memOp : ops) {
          offsets[currOffset++] = offset;
          int allocationOffset = offset;
          int actualLen = 0;
          for (byte[] record : memOp.records) {
            actualLen += record.length;
          }

          actualLen = 4 + (4 * memOp.records.length) + actualLen;
          bytes[offset] = (byte) (allocateCount == 1 ? 1 : 0); //single allocation
          offset += 1;
          bytes[offset] = 0; //freed
          offset += 1;
          offset += 8; //outer address
          DataUtils.intToBytes(allocationOffset, bytes, offset); // offset from top of page allocation
          offset += 4;
          offset += 8; // update time
          DataUtils.intToBytes(actualLen, bytes, offset); //actual size
          offset += 4;

          DataUtils.intToBytes(memOp.records.length, bytes, offset);
          offset += 4;
          for (byte[] record : memOp.records) {
            DataUtils.intToBytes(record.length, bytes, offset);
            offset += 4;
            System.arraycopy(record, 0, bytes, offset, record.length);
            offset += record.length;
          }
        }

        Unsafe unsafe = AddressMap.getUnsafe();
        long address = unsafe.allocateMemory(totalSize);

        if (allocateCount > 1) {
          long pageId = server.getAddressMap().addPage(address, totalSize);

          DataUtils.longToBytes(pageId, bytes, 0);
          DataUtils.intToBytes(allocateCount, bytes, 8);
        }

        for (int j = 0; j < bytes.length; j++) {
          unsafe.putByte(address + j, bytes[j]);
        }

        currOffset = 0;
        for (MemoryOp memOp : ops) {
          memOp.address = server.getAddressMap().addAddress(address + offsets[currOffset]);
          DataUtils.longToAddress(memOp.address, address + offsets[currOffset] + 2, unsafe);
          currOffset++;
        }
      }
    }
    this.ops = allOps;
    phase = 1;
  }

  public void addAll(MemoryOps currOps) {
    for (MemoryOp op : currOps.ops) {
      ops.add(op);
    }
  }

  public int phase() {
    return this.phase;
  }
}
