package com.sonicbase.database;

import com.sonicbase.util.ISO8601;
import org.testng.annotations.Test;

import java.util.Date;

/**
 * Created by lowryda on 6/17/17.
 */
public class TestLite {

  @Test
  public void test() {
    System.out.println(ISO8601.to8601String(new Date(System.currentTimeMillis())));
  }
}
