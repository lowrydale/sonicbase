package com.sonicbase.query;

public interface Statement {

  BinaryExpression createBinaryExpression(String columnName, BinaryExpression.Operator op, long value);

  BinaryExpression createBinaryExpression(String columnName, BinaryExpression.Operator op, String value);

  BinaryExpression createBinaryExpression(Expression leftExpression, BinaryExpression.Operator op, Expression rightExpression);

  InExpression createInExpression();

}
