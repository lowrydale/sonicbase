/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.codec.binary.Hex;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

  public enum Function {
    ceiling(new CeilingFunction()),
    floor(new FloorFunction()),
    abs(new AbsFunction()),
    str(new StrFunction()),
    avg(new AvgFunction()),
    max(new MaxFunction()),
    max_timestamp(new MaxTimestampFunction()),
    sum(new SumFunction()),
    min(new MinFunction()),
    min_timestamp(new MinTImestampFunction()),
    bit_shift_left(new BitShiftLeftFunction()),
    bit_shift_right(new BitShiftRightFunction()),
    bit_and(new BitAndFunction()),
    bit_not(new BitNotFunction()),
    bit_or(new BitOrFunction()),
    bit_xor(new BitXOrFunction()),
    coalesce(new CoalesceFunction()),
    char_length(new CharLengthFunction()),
    concat(new ConcatFunction()),
    now(new NowFunction()),
    date_add(new DateAddFunction()),
    day(new DayFunction()),
    day_of_week(new DayOfWeekFunction()),
    day_of_year(new DayOfYearFunction()),
    minute(new MinuteFunction()),
    month(new MonthFunction()),
    second(new SecondFunction()),
    hour(new HourFunction()),
    week_of_month(new WeekOfMonthFunction()),
    week_of_year(new WeekOfYearFunction()),
    year(new YearFunction()),
    power(new PowerFunction()),
    hex(new HexFunction()),
    log(new LogFunction()),
    log10(new Log10Function()),
    mod(new ModFunction()),
    lower(new LowerFunction()),
    upper(new UpperFunction()),
    index_of(new IndexOfFunction()),
    replace(new ReplaceFunction()),
    round(new RoundFunction()),
    pi(new PiFunction()),
    sqrt(new SqrtFunction()),
    tan(new TanFunction()),
    cos(new CosFunction()),
    sin(new SinFunction()),
    cot(new CotFunction()),
    trim(new TrimFunction()),
    radians(new RadiansFunction()),
    custom(new CustomFunction()),
    is_null(new IsNullFunction());

    private final FunctionBase func;

    Function(FunctionBase func) {
      this.func = func;
    }
  }

  interface FunctionBase {
    Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms);
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

  static class MethodObject {
    Method method;
    Object obj;

    public MethodObject(Method method, Object obj) {
      this.method = method;
      this.obj = obj;
    }
  }
  static class CustomFunction implements FunctionBase {

    private Map<String, MethodObject> methods = new ConcurrentHashMap<>();
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      try {
        Object classNameObj = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
        String className = null;
        if (classNameObj instanceof byte[]) {
          className = new String((byte[])classNameObj, "utf-8");
        }
        else {
          className = (String) classNameObj;
        }

        Object methodNameObj = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
        String methodName = null;
        if (methodNameObj instanceof byte[]) {
          methodName = new String((byte[])methodNameObj, "utf-8");
        }
        else {
          methodName = (String) methodNameObj;
        }

        Object[] evaluatedParms = new Object[funcParms.size() - 2];
        for (int i = 2; i < funcParms.size(); i++) {
          evaluatedParms[i - 2] = funcParms.get(i).evaluateSingleRecord(tableSchemas, records, parms);
        }

        String key = className + "." + methodName;
        MethodObject methodObj = methods.get(key);
        Object obj = null;
        Method method = null;
        if (methodObj == null) {
          obj = Class.forName(className).newInstance();
          method = obj.getClass().getMethod(methodName, Object[].class);
          methods.put(key, new MethodObject(method, obj));
        }
        else {
          obj = methodObj.obj;
          method = methodObj.method;
        }

        return method.invoke(obj, new Object[]{evaluatedParms});
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  static class CeilingFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return Math.ceil((Double) DataType.getDoubleConverter().convert(parm));
    }
  }

  static class FloorFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return Math.floor((Double) DataType.getDoubleConverter().convert(parm));
    }
  }

  static class AbsFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
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
  }

  static class StrFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      return String.valueOf(parm);
    }
  }

  static class AvgFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Double sum = 0d;
      int count = 0;
      for (ExpressionImpl expression : funcParms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value == null) {
          continue;
        }
        count++;
        sum += (Double) DataType.getDoubleConverter().convert(value);
      }
      return sum / count;
    }
  }

  static class MaxFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Double max = null;
      for (ExpressionImpl expression : funcParms) {
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
  }

  static class MaxTimestampFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Timestamp max = null;
      for (ExpressionImpl expression : funcParms) {
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
  }

  static class SumFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Double sum = null;
      for (ExpressionImpl expression : funcParms) {
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
  }

  static class MinFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Double min = null;
      for (ExpressionImpl expression : funcParms) {
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
  }

  static class MinTImestampFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Timestamp min = null;
      for (ExpressionImpl expression : funcParms) {
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
  }

  static class BitShiftLeftFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      Object numBits = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (numBits == null) {
        return null;
      }
      Long value = (Long) DataType.getLongConverter().convert(parm);
      Integer bits = (int) (long)(Long)DataType.getLongConverter().convert(numBits);
      return value << bits;
    }
  }

  static class BitShiftRightFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object parm = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (parm == null) {
        return null;
      }
      Object numBits = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (numBits == null) {
        return null;
      }
      Long value = (Long) DataType.getLongConverter().convert(parm);
      Integer bits = (int) (long)(Long)DataType.getLongConverter().convert(numBits);
      return value >>> bits;
    }
  }

  static class BitAndFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits & rhsBits;
    }
  }

  static class BitNotFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      return ~lhsBits;
    }
  }

  static class BitOrFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits | rhsBits;
    }
  }

  static class BitXOrFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Long lhsBits = (Long) DataType.getLongConverter().convert(lhs);
      Long rhsBits = (Long)DataType.getLongConverter().convert(rhs);
      return lhsBits ^ rhsBits;
    }
  }

  static class CoalesceFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      for (ExpressionImpl expression : funcParms) {
        Object value = expression.evaluateSingleRecord(tableSchemas, records, parms);
        if (value != null) {
          return value;
        }
      }
      return null;
    }
  }

  static class CharLengthFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String lhsValue = (String) DataType.getStringConverter().convert(lhs);
      return lhsValue.length();
    }
  }

  static class ConcatFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      return lhsStr + rhsStr;
    }
  }

  static class NowFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Timestamp time = new Timestamp(System.currentTimeMillis());
      return time;
    }
  }

  static class DateAddFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
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
  }

  static class DayFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_MONTH);
    }
  }

  static class DayOfWeekFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_WEEK);
    }
  }

  static class DayOfYearFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.DAY_OF_YEAR);
    }
  }

  static class MinuteFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.MINUTE);
    }
  }

  static class MonthFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.MONTH);
    }
  }

  static class SecondFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.SECOND);
    }
  }

  static class HourFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.HOUR_OF_DAY);
    }
  }

  static class WeekOfMonthFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.WEEK_OF_MONTH);
    }
  }

  static class WeekOfYearFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.WEEK_OF_YEAR);
    }
  }

  static class YearFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Timestamp time = (Timestamp) DataType.getTimestampConverter().convert(lhs);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time.getTime());
      return cal.get(Calendar.YEAR);
    }
  }

  static class PowerFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Double lhsValue = (Double) DataType.getDoubleConverter().convert(lhs);
      Double rhsValue = (Double) DataType.getDoubleConverter().convert(rhs);
      return Math.pow(lhsValue, rhsValue);
    }
  }

  static class HexFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
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
  }

  static class LogFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.log(value);
    }
  }

  static class Log10Function implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.log10(value);
    }
  }

  static class ModFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Double lhsValue = (Double) DataType.getDoubleConverter().convert(lhs);
      Double rhsValue = (Double) DataType.getDoubleConverter().convert(rhs);
      return lhsValue % rhsValue;
    }
  }

  static class LowerFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.toLowerCase();
    }
  }

  static class UpperFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.toUpperCase();
    }
  }

  static class IndexOfFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      return lhsStr.indexOf(rhsStr);
    }
  }

  static class ReplaceFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Object rhs = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
      if (rhs == null) {
        return null;
      }
      Object replace = funcParms.get(2).evaluateSingleRecord(tableSchemas, records, parms);
      if (replace == null) {
        return null;
      }
      String lhsStr = (String) DataType.getStringConverter().convert(lhs);
      String rhsStr = (String) DataType.getStringConverter().convert(rhs);
      String replaceStr = (String) DataType.getStringConverter().convert(replace);
      return lhsStr.replace(rhsStr, replaceStr);
    }
  }

  static class RoundFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.round(value);
    }
  }

  static class PiFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      return Math.PI;
    }
  }

  static class SqrtFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.sqrt(value);
    }
  }

  static class TanFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.tan(value);
    }
  }

  static class CosFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.cos(value);
    }
  }

  static class SinFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.sin(value);
    }
  }

  static class CotFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return 1 / Math.tan(value);
    }
  }

  static class TrimFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      String str = (String) DataType.getStringConverter().convert(lhs);
      return str.trim();
    }
  }

  static class RadiansFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return null;
      }
      Double value = (Double) DataType.getDoubleConverter().convert(lhs);
      return Math.toRadians(value);
    }
  }

  static class IsNullFunction implements FunctionBase {
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      Object lhs = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
      if (lhs == null) {
        return true;
      }
      return false;
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    Function funcValue = null;
    try {
      funcValue = Function.valueOf(name.toLowerCase());
    }
    catch (Exception e) {
      throw new DatabaseException("Invalid function: name=" + name, e);
    }
    return funcValue.func.evaluate(tableSchemas, records, parms, this.parms);
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset) {
    return null;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset, boolean evaluateExpression, boolean analyze) {
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
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
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
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
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
