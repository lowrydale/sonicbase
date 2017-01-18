package com.lowryengineering.database.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for
 */
public interface BinaryExpression extends Expression {
  Map<Integer, Operator> idToOperator = new HashMap<Integer, Operator>();


  enum Operator {
    equal(0),
    less(1),
    greater(2),
    lessEqual(3),
    greaterEqual(4),
    and(5),
    or(6),
    notEqual(7),
    like(8);

    private final int id;

    Operator(int id) {
      this.id = id;
      idToOperator.put(id, this);
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
