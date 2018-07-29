package com.sonicbase.query;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface Statement {

  BinaryExpression createBinaryExpression(String columnName, BinaryExpression.Operator op, long value);

  BinaryExpression createBinaryExpression(String columnName, BinaryExpression.Operator op, String value);

  BinaryExpression createBinaryExpression(Expression leftExpression, BinaryExpression.Operator op, Expression rightExpression);

  InExpression createInExpression();

}
