/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;

public class AssertUtils {
  private AssertUtils() {

  }

  public static void assertEquals(Object o1, Object o2) {
    if (!o1.equals(o2)) {
      throw new DatabaseException("Not equal");
    }
  }

  public static void assertTrue(Object o1) {
    if (!o1.equals(true)) {
      throw new DatabaseException("not true");
    }
  }

  public static void assertFalse(Object o1) {
    if (o1.equals(true)) {
      throw new DatabaseException("not false");
    }
  }
}
