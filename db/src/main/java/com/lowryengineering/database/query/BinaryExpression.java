package com.lowryengineering.database.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for
 */
public interface BinaryExpression extends Expression {
  Map<Integer, Operator> idToOperator = new HashMap<Integer, Operator>();


  enum Operator {
    equal(0, "="),
    less(1, "<"),
    greater(2, ">"),
    lessEqual(3, "<="),
    greaterEqual(4, ">="),
    and(5, "and"),
    or(6, "or"),
    notEqual(7, "!="),
    like(8, "like");

    private final int id;
    private final String symbol;

    Operator(int id, String symbol) {
      this.id = id;
      this.symbol = symbol;
      idToOperator.put(id, this);
    }

    public String getSymbol() {
      return symbol;
    }

    public int getId() {
      return id;
    }

    public static Operator getOperator(int id) {
      return idToOperator.get(id);
    }

    public boolean isRelationalOp() {
      return this == equal || this == less || this == greater || this == lessEqual || this == greaterEqual || this == notEqual;
    }
  }

  void setLeftExpression(Expression expression);

  void setRightExpression(Expression expression);

  Expression getLeftExpression();

  Expression getRightExpression();
}
