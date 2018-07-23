package com.sonicbase.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for
 */
public interface BinaryExpression extends Expression {
  Map<Integer, Operator> idToOperator = new HashMap<>();

  enum Operator {
    EQUAL(0, "="),
    LESS(1, "<"),
    GREATER(2, ">"),
    LESS_EQUAL(3, "<="),
    GREATER_EQUAL(4, ">="),
    AND(5, "and"),
    OR(6, "or"),
    NOT_EQUAL(7, "!="),
    LIKE(8, "like"),
    PLUS(9, "+"),
    MINUS(10, "-"),
    TIMES(11, "*"),
    DIVIDE(12, "/"),
    BITWISE_AND(13, "&"),
    BITWISE_OR(14, "|"),
    BITWISE_X_OR(15, "^"),
    MODULO(16, "%");

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
      return this == EQUAL || this == LESS || this == GREATER || this == LESS_EQUAL || this == GREATER_EQUAL || this == NOT_EQUAL;
    }
  }

  void setLeftExpression(Expression expression);

  void setRightExpression(Expression expression);

  Expression getLeftExpression();

  Expression getRightExpression();
}
