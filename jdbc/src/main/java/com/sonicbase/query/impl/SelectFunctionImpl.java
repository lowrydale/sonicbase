package com.sonicbase.query.impl;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
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
