package com.lowryengineering.database.query;

import java.io.UnsupportedEncodingException;

/**
 * Responsible for
 */
public interface InExpression extends Expression {

  void setColumn(String tableName, String columnName, String alias);

  void addValue(String value) throws UnsupportedEncodingException;

  void addValue(long value);

}
