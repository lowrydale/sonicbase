package com.sonicbase.bench;

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

  public Object avg(Object[] o) {
    Double lhs = (Double) DataType.getDoubleConverter().convert(o[0]);
    Double rhs = (Double) DataType.getDoubleConverter().convert(o[1]);
    return (lhs + rhs) / 2;
  }

  public Object min(Object[] o) {
    Double lhs = (Double) DataType.getDoubleConverter().convert(o[0]);
    Double rhs = (Double) DataType.getDoubleConverter().convert(o[1]);
    return Math.min(lhs, rhs);
  }
}
