package com.sonicbase.database;

import com.sonicbase.common.ComObject;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.sonicbase.common.ComObject.Type.*;
import static com.sonicbase.common.ComObject.Type.byteArrayType;
import static com.sonicbase.common.ComObject.Type.intType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by lowryda on 6/24/17.
 */
public class TestComObject {

  @Test
  public void test() {

    ComObject cobj = new ComObject();

    cobj.put(ComObject.Tag.serializationVersion, 5544332211L);
    cobj.put(ComObject.Tag.tableName, "myTable");
    cobj.put(ComObject.Tag.isExcpliciteTrans, true);
    cobj.put(ComObject.Tag.recordLength, 999);
    cobj.put(ComObject.Tag.keyBytes, new byte[]{1, 2, 3});

    byte[] bytes = cobj.serialize();

    cobj = new ComObject();
    cobj.deserialize(bytes);
    assertEquals((long)cobj.getLong(ComObject.Tag.serializationVersion), 5544332211L);
    assertEquals(cobj.getString(ComObject.Tag.tableName), "myTable");
    assertEquals((boolean)cobj.getBoolean(ComObject.Tag.isExcpliciteTrans), true);
    assertEquals((int)cobj.getInt(ComObject.Tag.recordLength), 999);
    assertTrue(Arrays.equals(cobj.getByteArray(ComObject.Tag.keyBytes), new byte[]{1, 2, 3}));
  }
}