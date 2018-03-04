package com.sonicbase.server;

import com.sonicbase.util.DateUtils;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import static org.testng.Assert.assertEquals;

public class TestDateUtils {

  @Test
  public void testBasics() throws ParseException {
    Date date = new Date(2018-1990, 1, 1, 1, 1, 1);
    String str = DateUtils.toString(date);
    assertEquals(str, "1928-02-01T08_01_01+0000");
    date = DateUtils.fromString(str);
   // assertEquals(date.getTimezoneOffset(), 0);
    assertEquals(date.getYear(), 2018-1990);
    assertEquals(date.getMonth(), 1);
    assertEquals(date.getDate(), 1);
    assertEquals(date.getHours(), 1);
    assertEquals(date.getMinutes(), 1);
    assertEquals(date.getSeconds(), 1);

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(-(cal.get(Calendar.ZONE_OFFSET) +
        cal.get(Calendar.DST_OFFSET)) / (60 * 1000), 420);
  }
}
