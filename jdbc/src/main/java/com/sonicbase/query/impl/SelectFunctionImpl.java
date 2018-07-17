package com.sonicbase.query.impl;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

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
