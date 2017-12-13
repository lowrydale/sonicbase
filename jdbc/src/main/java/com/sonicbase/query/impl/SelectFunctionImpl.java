/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class SelectFunctionImpl {
  private String name;
  private ExpressionList parms;

  public SelectFunctionImpl(String function, ExpressionList parameters) {
    this.name = function;
    this.parms = parameters;
  }

  public String getName() {
    return name;
  }

  public ExpressionList getParms() {
    return parms;
  }

}
