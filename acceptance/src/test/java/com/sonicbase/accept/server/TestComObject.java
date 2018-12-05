package com.sonicbase.accept.server;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComArrayOld;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.ComObjectOld;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestComObject {

  @Test
  public void test() {

    ComObject cobj = new ComObject(5);

    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, (short)554);
    cobj.put(ComObject.Tag.TABLE_NAME, "myTable");
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
    cobj.put(ComObject.Tag.RECORD_LENGTH, 999);
    cobj.put(ComObject.Tag.KEY_BYTES, new byte[]{1, 2, 3});

    byte[] bytes = cobj.serialize();

    cobj = new ComObject(bytes);
    assertEquals((short)cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), 554);
    assertEquals(cobj.getString(ComObject.Tag.TABLE_NAME), "myTable");
    assertEquals((boolean)cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS), true);
    assertEquals((int)cobj.getInt(ComObject.Tag.RECORD_LENGTH), 999);
    assertTrue(Arrays.equals(cobj.getByteArray(ComObject.Tag.KEY_BYTES), new byte[]{1, 2, 3}));
  }

  private static final int ARRAY_LEN = 500;

  @Test
  public void testPerfOld() {
    long begin = System.currentTimeMillis();

    String table = "myTable";
    byte[] sbytes = new byte[]{1, 2, 3};
    for (int i = 0; i < 1_000; i++) {
      ComObjectOld cobj = new ComObjectOld(6);

      cobj.put(ComObjectOld.Tag.SERIALIZATION_VERSION, (short) 554);
      cobj.put(ComObjectOld.Tag.TABLE_NAME, table);
      cobj.put(ComObjectOld.Tag.IS_EXCPLICITE_TRANS, true);
      cobj.put(ComObjectOld.Tag.RECORD_LENGTH, 999);
      cobj.put(ComObjectOld.Tag.KEY_BYTES, sbytes);

      ComArrayOld array = cobj.putArray(ComObjectOld.Tag.RECORDS, ComObjectOld.Type.OBJECT_TYPE, ARRAY_LEN);
      for (int j = 0; j < ARRAY_LEN; j++) {
        ComObjectOld icobj = new ComObjectOld(5);

        icobj.put(ComObjectOld.Tag.SERIALIZATION_VERSION, (short) 554);
        icobj.put(ComObjectOld.Tag.TABLE_NAME, table);
        icobj.put(ComObjectOld.Tag.IS_EXCPLICITE_TRANS, true);
        icobj.put(ComObjectOld.Tag.RECORD_LENGTH, 999);
        icobj.put(ComObjectOld.Tag.KEY_BYTES, sbytes);
        array.add(icobj);
      }

      byte[] bytes = cobj.serialize();

      cobj = new ComObjectOld(bytes);
      assertEquals((short) cobj.getShort(ComObjectOld.Tag.SERIALIZATION_VERSION), 554);
      assertEquals(cobj.getString(ComObjectOld.Tag.TABLE_NAME), table);
      assertEquals((boolean) cobj.getBoolean(ComObjectOld.Tag.IS_EXCPLICITE_TRANS), true);
      assertEquals((int) cobj.getInt(ComObjectOld.Tag.RECORD_LENGTH), 999);
      assertTrue(Arrays.equals(cobj.getByteArray(ComObjectOld.Tag.KEY_BYTES), sbytes));
      array = cobj.getArray(ComObjectOld.Tag.RECORDS);
      for (int j = 0; j < ARRAY_LEN; j++) {
        ComObjectOld icobj = (ComObjectOld) array.getArray().get(j);
        assertEquals((short) icobj.getShort(ComObjectOld.Tag.SERIALIZATION_VERSION), 554);
        assertEquals(icobj.getString(ComObjectOld.Tag.TABLE_NAME), table);
        assertEquals((boolean) icobj.getBoolean(ComObjectOld.Tag.IS_EXCPLICITE_TRANS), true);
        assertEquals((int) icobj.getInt(ComObjectOld.Tag.RECORD_LENGTH), 999);
        assertTrue(Arrays.equals(icobj.getByteArray(ComObjectOld.Tag.KEY_BYTES), sbytes));
      }
    }
    System.out.println("duration=" + (System.currentTimeMillis() - begin));
  }

  @Test
  public void testPerfNew() {
    long begin = System.currentTimeMillis();
    String table = "myTable";
    byte[] sbytes = new byte[]{1, 2, 3};
    for (int i = 0; i < 1_000; i++) {
      ComObject cobj = new ComObject(6);

      cobj.put(ComObject.Tag.SERIALIZATION_VERSION, (short) 554);
      cobj.put(ComObject.Tag.TABLE_NAME, table);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
      cobj.put(ComObject.Tag.RECORD_LENGTH, 999);
      cobj.put(ComObject.Tag.KEY_BYTES, sbytes);

      ComArray array = cobj.putArray(ComObject.Tag.RECORDS, ComObject.Type.OBJECT_TYPE, ARRAY_LEN);
      for (int j = 0; j < ARRAY_LEN; j++) {
        ComObject icobj = new ComObject(5);

        icobj.put(ComObject.Tag.SERIALIZATION_VERSION, (short) 554);
        icobj.put(ComObject.Tag.TABLE_NAME, table);
        icobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
        icobj.put(ComObject.Tag.RECORD_LENGTH, 999);
        icobj.put(ComObject.Tag.KEY_BYTES, sbytes);
        array.add(icobj);
      }

      byte[] bytes = cobj.serialize();

      cobj = new ComObject(bytes);
      assertEquals((short) cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), 554);
      assertEquals(cobj.getString(ComObject.Tag.TABLE_NAME), table);
      assertEquals((boolean) cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS), true);
      assertEquals((int) cobj.getInt(ComObject.Tag.RECORD_LENGTH), 999);
      assertTrue(Arrays.equals(cobj.getByteArray(ComObject.Tag.KEY_BYTES), sbytes));
      array = cobj.getArray(ComObject.Tag.RECORDS);
      for (int j = 0; j < ARRAY_LEN; j++) {
        ComObject icobj = (ComObject) array.getArray().get(j);
        assertEquals((short) icobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), 554);
        assertEquals(icobj.getString(ComObject.Tag.TABLE_NAME), table);
        assertEquals((boolean) icobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS), true);
        assertEquals((int) icobj.getInt(ComObject.Tag.RECORD_LENGTH), 999);
        assertTrue(Arrays.equals(icobj.getByteArray(ComObject.Tag.KEY_BYTES), sbytes));
      }
    }
    System.out.println("duration=" + (System.currentTimeMillis() - begin));
  }
}
