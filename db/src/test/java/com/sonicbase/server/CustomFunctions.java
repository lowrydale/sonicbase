/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.schema.DataType;

public class CustomFunctions {

  public Object plus(Object[] o) {
    if (o[0] == null) {
      if (o[1] != null) {
        return o[1];
      }
      return null;
    }
    long lhs = (long) DataType.getLongConverter().convert(o[0]);
    if (o[1] == null) {
      return lhs;
    }
    long rhs = (long) DataType.getLongConverter().convert(o[1]);
    return lhs + rhs;
  }

}
