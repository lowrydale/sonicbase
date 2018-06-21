/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ComObjectTest {

  @Test
  public void testString() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName,"test");
    assertEquals(cobj.getString(ComObject.Tag.dbName), "test");
  }
}
