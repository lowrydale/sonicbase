/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertEquals;

public class BlobTest {
  @Test
  public void testBlob() throws SQLException {

    byte[] bytes = new byte[]{1,2,3,4,5,6};
    Blob blob = new Blob(bytes);
    assertEquals(blob.length(), 6L);
  }
}
