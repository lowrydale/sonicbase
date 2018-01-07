package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by lowryda on 6/24/17.
 */
public class TestComObject {

  @Test
  public void test() {

    ComObject cobj = new ComObject();

    cobj.put(ComObject.Tag.serializationVersion, (short)554);
    cobj.put(ComObject.Tag.tableName, "myTable");
    cobj.put(ComObject.Tag.isExcpliciteTrans, true);
    cobj.put(ComObject.Tag.recordLength, 999);
    cobj.put(ComObject.Tag.keyBytes, new byte[]{1, 2, 3});

    byte[] bytes = cobj.serialize();

    cobj = new ComObject();
    cobj.deserialize(bytes);
    assertEquals((short)cobj.getShort(ComObject.Tag.serializationVersion), 554);
    assertEquals(cobj.getString(ComObject.Tag.tableName), "myTable");
    assertEquals((boolean)cobj.getBoolean(ComObject.Tag.isExcpliciteTrans), true);
    assertEquals((int)cobj.getInt(ComObject.Tag.recordLength), 999);
    assertTrue(Arrays.equals(cobj.getByteArray(ComObject.Tag.keyBytes), new byte[]{1, 2, 3}));
  }
}
