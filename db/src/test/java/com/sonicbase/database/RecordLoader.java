/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database;

import java.io.IOException;
import java.sql.SQLException;

public class RecordLoader {

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    com.sonicbase.misc.RecordLoader loader = new com.sonicbase.misc.RecordLoader();
    loader.main(args);
  }
}
