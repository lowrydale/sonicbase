/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.database;

import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TestTime {

  @Test
  public void test() {
    DateFormat writeFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
    Date date = null;

    if (date != null) {
      String formattedDate = writeFormat.format(date);
    }
  }
}
