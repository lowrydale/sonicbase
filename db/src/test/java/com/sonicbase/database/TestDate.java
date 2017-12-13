/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database;

import com.sonicbase.util.DateUtils;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Date;

public class TestDate {

  @Test
  public void test() throws ParseException {
    Date date = DateUtils.fromString("2018-12-08T03:57:46.915");
    System.out.println(date.toString());
    date = DateUtils.fromString("2018-12-08T03:57:46");
    System.out.println(date.toString());
    System.out.println(DateUtils.toString(date));
    date = DateUtils.fromString("2018-12-08T10_57_46+0000");
    System.out.println(date.toString());
  }
}
