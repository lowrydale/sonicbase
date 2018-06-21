package com.sonicbase.accept.server;

import com.sonicbase.query.DatabaseException;
import org.testng.annotations.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

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
