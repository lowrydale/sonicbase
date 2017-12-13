/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.Parameter;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.codec.binary.Hex;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class FunctionImpl extends ExpressionImpl {
  private String name;
  private List<ExpressionImpl> parms;

  public FunctionImpl(String function, List<ExpressionImpl> parameters) {
    this.name = function;
    this.parms = parameters;
  }

  public FunctionImpl() {

  }

  public String getName() {
    return name;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {

  }

  @Override
  public Type getType() {
    return Type.function;
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    if (name.equalsIgnoreCase("ceiling")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return Math.ceil((Double) DataType.getDoubleConverter().convert(parm));
    }
    if (name.equalsIgnoreCase("floor")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return Math.floor((Double) DataType.getDoubleConverter().convert(parm));
    }
    if (name.equalsIgnoreCase("abs")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      if (parm instanceof Integer || parm instanceof Short || parm instanceof Byte || parm instanceof Long) {
        return Math.abs((Long)DataType.getLongConverter().convert(parm));
      }
      if (parm instanceof Float || parm instanceof Double) {
        return Math.abs((Double) DataType.getDoubleConverter().convert(parm));
      }
      if (parm instanceof BigDecimal) {
        BigDecimal bd = (BigDecimal) parm;
        return bd.abs();
      }
      return null;
    }
    if (name.equalsIgnoreCase("str")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return String.valueOf(parm);
    }
    if (name.equalsIgnoreCase("avg")) {
      Double sum = 0d;
      int count = 0;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        count++;
        sum += (Double) DataType.getDoubleConverter().convert(value);
      }
      return sum / count;
    }
    if (name.equalsIgnoreCase("max")) {
      Double max = null;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        Double currValue = (Double) DataType.getDoubleConverter().convert(value);
        if (max == null) {
          max = currValue;
        }
        else {
          max = Math.max(max, currValue);
        }
      }
      return max;
    }
    if (name.equalsIgnoreCase("max_timestamp")) {
      Timestamp max = null;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        Timestamp currValue = (Timestamp) DataType.getTimestampConverter().convert(value);
        if (max == null) {
          max = currValue;
        }
        else {
          if (currValue.compareTo(max) > 0) {
            max = currValue;
          }
        }
      }
      return max;
    }
    if (name.equalsIgnoreCase("sum")) {
      Double sum = null;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        Double currValue = (Double) DataType.getDoubleConverter().convert(value);
        if (sum == null) {
          sum = currValue;
        }
        else {
          sum += currValue;
        }
      }
      return sum;
    }
    if (name.equalsIgnoreCase("min")) {
      Double min = null;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        Double currValue = (Double) DataType.getDoubleConverter().convert(value);
        if (min == null) {
          min = currValue;
        }
        else {
          min = Math.min(min, currValue);
        }
      }
      return min;
    }
    if (name.equalsIgnoreCase("min_timestamp")) {
      Timestamp min = null;
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        Timestamp currValue = (Timestamp) DataType.getTimestampConverter().convert(value);
        if (min == null) {
          min = currValue;
        }
        else {
          if (currValue.compareTo(min) < 0) {
            min = currValue;
          }
        }
      }
      return min;
    }
    if (name.equalsIgnoreCase("bit_shift_left")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      Object numBits = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (numBits == null) {
        return null;
      }
      Long value = (Long) DataType.getLongConverter().convert(parm);
      Integer bits = (int) (long)(Long)DataType.getLongConverter().convert(numBits);
      return value << bits;
    }
    if (name.equalsIgnoreCase("bit_shift_right")) {
      Object parm = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      Object numBits = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (numBits == null) {
        return null;
      }
      Long value = (Long) DataType.getLongConverter().convert(parm);
      Integer bits = (int) (long)(Long)DataType.getLongConverter().convert(numBits);
      return value >>> bits;
    }
    if (name.equalsIgnoreCase("bit_and")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits & rhsBits;
    }
    if (name.equalsIgnoreCase("bit_not")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      return ~lhsBits;
    }
    if (name.equalsIgnoreCase("bit_or")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits | rhsBits;
    }
    if (name.equalsIgnoreCase("bit_xor")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits ^ rhsBits;
    }
    if (name.equalsIgnoreCase("coalesce")) {
      for (ExpressionImpl expression : this.parms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value != null) {
          return value;
        }
      }
      return null;
    }
    if (name.equalsIgnoreCase("char_length")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String lhsValue = (String) DataType.getStringConverter().convert(lhs);
      return lhsValue.length();
    }
    if (name.equalsIgnoreCase("concat")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      return lhsStr + rhsStr;
    }
    if (name.equalsIgnoreCase("now")) {
      Timestamp time = new Timestamp(System.currentTimeMillis());
      return time;
    }
    if (name.equalsIgnoreCase("date_add")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Long delta = (Long) DataType.getLongConverter().convert(rhs);
      long timeMillis = time.getTime();
      timeMillis += delta;
      Timestamp ret = new Timestamp(timeMillis);
      return ret;
    }
    if (name.equalsIgnoreCase("day")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_MONTH);
    }
    if (name.equalsIgnoreCase("day_of_week")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_WEEK);
    }
    if (name.equalsIgnoreCase("day_of_year")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_YEAR);
    }
    if (name.equalsIgnoreCase("minute")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.MINUTE);
    }
    if (name.equalsIgnoreCase("month")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.MONTH);
    }
    if (name.equalsIgnoreCase("second")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.SECOND);
    }
    if (name.equalsIgnoreCase("hour")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.HOUR_OF_DAY);
    }
    if (name.equalsIgnoreCase("week_of_month")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.WEEK_OF_MONTH);
    }
    if (name.equalsIgnoreCase("week_of_year")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.WEEK_OF_YEAR);
    }
    if (name.equalsIgnoreCase("year")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.YEAR);
    }
    if (name.equalsIgnoreCase("power")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Double lhsValue = (Double) DataType.getDoubleConverter().convert(lhs);
      Double rhsValue = (Double) DataType.getDoubleConverter().convert(rhs);
      return Math.pow(lhsValue, rhsValue);
    }
    if (name.equalsIgnoreCase("hex")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      try {
        String str = (String) DataType.getStringConverter().convert(lhs);
        Hex hex = new Hex("utf-8");
        return new String(hex.encode(str.getBytes("utf-8")), "utf-8").toUpperCase();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    if (name.equalsIgnoreCase("log")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.log(value);
    }
    if (name.equalsIgnoreCase("log10")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.log10(value);
    }
    if (name.equalsIgnoreCase("mod")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Double lhsValue = (Double) DataType.getDoubleConverter().convert(lhs);
      Double rhsValue = (Double) DataType.getDoubleConverter().convert(rhs);
      return lhsValue % rhsValue;
    }
    if (name.equalsIgnoreCase("lower")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.toLowerCase();
    }
    if (name.equalsIgnoreCase("upper")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.toUpperCase();
    }
    if (name.equalsIgnoreCase("pi")) {
      return Math.PI;
    }
    if (name.equalsIgnoreCase("index_of")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      return lhsStr.indexOf(rhsStr);
    }
    if (name.equalsIgnoreCase("replace")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = this.parms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Object replace = this.parms.get(2).evaluateSingleRecord(tableSchemas, records, parms);
      if (replace == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      String replaceStr = (String) DataType.getStringConverter().convert(replace);
      return lhsStr.replace(rhsStr, replaceStr);
    }
    if (name.equalsIgnoreCase("round")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.round(value);
    }
    if (name.equalsIgnoreCase("sqrt")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.sqrt(value);
    }
    if (name.equalsIgnoreCase("tan")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.tan(value);
    }
    if (name.equalsIgnoreCase("cos")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.cos(value);
    }
    if (name.equalsIgnoreCase("sin")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.sin(value);
    }
    if (name.equalsIgnoreCase("cot")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return 1 / Math.tan(value);
    }
    if (name.equalsIgnoreCase("trim")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.trim();
    }
    if (name.equalsIgnoreCase("radians")) {
      Object lhs = this.parms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.toRadians(value);
    }

    throw new DatabaseException("Invalid function");
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset) {
    return null;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset, boolean evaluateExpression) {
    return null;
  }

  @Override
  public boolean canUseIndex() {
    return false;
  }

  @Override
  public boolean canSortWithIndex() {
    return false;
  }

  @Override
  public void queryRewrite() {

  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    return null;
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      name = in.readUTF();
      int count = in.readInt();
      parms = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        ExpressionImpl expression = ExpressionImpl.deserializeExpression(in);
        parms.add(expression);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void serialize(DataOutputStream out) {
    try {
      super.serialize(out);
      out.writeUTF(name);
      out.writeInt(parms.size());
      for (ExpressionImpl expression : parms) {
        out.write(ExpressionImpl.serializeExpression(expression));
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


}
