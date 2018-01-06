/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database.unit;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.AddressMap;
import org.testng.annotations.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;

public class TestAdressMap {

  private Unsafe unsafe = getUnsafe();

  private static Unsafe getUnsafe() {
    try {

      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
  @Test
  public void test() {
//    AddressMap map = new AddressMap();
//    for (int i = 1; i < 100_000; i++) {
//      assertEquals(map.addAddress(i, 0), i);
//    }
//    for (int i = 1; i < 100_000; i++) {
//      map.removeAddress(i, unsafe);
//    }
//    for (int i = 1; i < 100_000; i++) {
//      assertEquals(map.addAddress(i, 0), i);
//    }
  }
}
