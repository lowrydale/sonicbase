package com.sonicbase.query;

import java.io.UnsupportedEncodingException;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface InExpression extends Expression {

  void setColumn(String tableName, String columnName, String alias);

  void addValue(String value) throws UnsupportedEncodingException;

  void addValue(long value);

}
