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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class FunctionImpl extends ExpressionImpl {
  private static final String UTF_8_STR = "utf-8";
  private String name;
  private List<ExpressionImpl> parms;

  public FunctionImpl(String function, List<ExpressionImpl> parameters) {
    this.name = function;
    this.parms = parameters;
  }

  FunctionImpl() {

  }


  private static final Map<String, Function> functionsByName = new HashMap<>();

  private static Function getFunction(String name) {
    return functionsByName.get(name);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name).append("(");
    for (int i = 0; i < parms.size(); i++) {
      if (i != 0) {
        builder.append(", ");
      }
      builder.append(parms.get(i).toString());
    }
    builder.append(")");
    return builder.toString();
  }

  public enum Function {
    CEILING("ceiling", new CeilingFunction()),
    FLOOR("floor", new FloorFunction()),
    ABS("abs", new AbsFunction()),
    STR("str", new StrFunction()),
    AVG("avg", new AvgFunction()),
    MAX("max", new MaxFunction()),
    MAX_TIMESTAMP("max_timestamp", new MaxTimestampFunction()),
    SUM("sum", new SumFunction()),
    MIN("min", new MinFunction()),
    MIN_TIMESTAMP("min_timestamp", new MinTimestampFunction()),
    BIT_SHIFT_LEFT("bit_shift_left", new BitShiftLeftFunction()),
    BIT_SHIFT_RIGHT("bit_shift_right", new BitShiftRightFunction()),
    BIT_AND("bit_and", new BitAndFunction()),
    BIT_NOT("bit_not", new BitNotFunction()),
    BIT_OR("bit_or", new BitOrFunction()),
    BIT_XOR("bit_xor", new BitXOrFunction()),
    COALESCE("coalesce", new CoalesceFunction()),
    CHAR_LENGTH("char_length", new CharLengthFunction()),
    CONCAT("concat", new ConcatFunction()),
    NOW("now", new NowFunction()),
    DATE_ADD("date_add", new DateAddFunction()),
    DAY("day", new DayFunction()),
    DAY_OF_WEEK("day_of_week", new DayOfWeekFunction()),
    DAY_OF_YEAR("day_of_year", new DayOfYearFunction()),
    MINUTE("minute", new MinuteFunction()),
    MONTH("month", new MonthFunction()),
    SECOND("second", new SecondFunction()),
    HOUR("hour", new HourFunction()),
    WEEK_OF_MONTH("week_of_month", new WeekOfMonthFunction()),
    WEEK_OF_YEAR("week_of_year", new WeekOfYearFunction()),
    YEAR("year", new YearFunction()),
    POWER("power", new PowerFunction()),
    HEX("hex", new HexFunction()),
    LOG("log", new LogFunction()),
    LOG10("log10", new Log10Function()),
    MOD("mod", new ModFunction()),
    LOWER("lower", new LowerFunction()),
    UPPER("upper", new UpperFunction()),
    INDEX_OF("index_of", new IndexOfFunction()),
    REPLACE("replace", new ReplaceFunction()),
    ROUND("round", new RoundFunction()),
    PI("pi", new PiFunction()),
    SQRT("sqrt", new SqrtFunction()),
    TAN("tan", new TanFunction()),
    COS("cos", new CosFunction()),
    SIN("sin", new SinFunction()),
    COT("cot", new CotFunction()),
    TRIM("trim", new TrimFunction()),
    RADIANS("radians", new RadiansFunction()),
    CUSTOM("custom", new CustomFunction()),
    IS_NULL("is_null", new IsNullFunction());

    private final FunctionBase func;

    Function(String name, FunctionBase func) {
      this.func = func;
      functionsByName.put(name, this);
    }
  }

  interface FunctionBase {
    Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms);
  }
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return Type.FUNCTION;
  }

  static class MethodObject {
    final Method method;
    final Object obj;

    MethodObject(Method method, Object obj) {
      this.method = method;
      this.obj = obj;
    }
  }
  static class CustomFunction implements FunctionBase {

    private final Map<String, MethodObject> methods = new ConcurrentHashMap<>();
    @Override
    public Object evaluate(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms, List<ExpressionImpl> funcParms) {
      try {
        Object classNameObj = funcParms.get(0).evaluateSingleRecord(tableSchemas, records, parms);
        String className;
        if (classNameObj instanceof byte[]) {
          className = new String((byte[])classNameObj, UTF_8_STR);
        }
        else {
          className = (String) classNameObj;
        }

        Object methodNameObj = funcParms.get(1).evaluateSingleRecord(tableSchemas, records, parms);
        String methodName;
        if (methodNameObj instanceof byte[]) {
          methodName = new String((byte[])methodNameObj, UTF_8_STR);
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
        Object obj;
        Method method;
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
      if (count == 0) {
        return 0;
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

  static class MinTimestampFunction implements FunctionBase {
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
      return new Timestamp(System.currentTimeMillis());
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
      return new Timestamp(timeMillis);
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
        Hex hex = new Hex(UTF_8_STR);
        return new String(hex.encode(str.getBytes(UTF_8_STR)), UTF_8_STR).toUpperCase();
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
      return lhs == null;
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    Function funcValue;
    try {
      funcValue = FunctionImpl.getFunction(name.toLowerCase());
    }
    catch (Exception e) {
      throw new DatabaseException("Invalid function: name=" + name, e);
    }
    return funcValue.func.evaluate(tableSchemas, records, parms, this.parms);
  }

  @Override
  public NextReturn next(SelectStatementImpl select, int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, int schemaRetryCount, AtomicBoolean didTableScan) {
    return null;
  }

  @Override
  public NextReturn next(SelectStatementImpl select, int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, boolean evaluateExpression, boolean analyze, int schemaRetryCount, AtomicBoolean didTableScan) {
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
  public ColumnImpl getPrimaryColumn() {
    return null;
  }

  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
      name = in.readUTF();
      int count = in.readInt();
      parms = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        ExpressionImpl expression = deserializeExpression(in);
        parms.add(expression);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
      out.writeUTF(name);
      out.writeInt(parms.size());
      for (ExpressionImpl expression : parms) {
        out.write(serializeExpression(expression));
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


}
