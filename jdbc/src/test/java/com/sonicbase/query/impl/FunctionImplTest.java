package com.sonicbase.query.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ClientTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FunctionImplTest {

  private Record[] records;
  private TableSchema[] tableSchemas;
  private final ParameterHandler parms = new ParameterHandler();

  @BeforeClass
  public void beforeClass() throws IOException {
    final TableSchema tableSchema = ClientTestUtils.createTable();
    tableSchemas = new TableSchema[]{tableSchema};

    DatabaseClient client = mock(DatabaseClient.class);

    DatabaseCommon common = ClientTestUtils.createCommon(tableSchema);
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);
    when(client.getCommon()).thenReturn(common);

    byte[][] recordBytes = ClientTestUtils.createRecords(common, tableSchema, 10);

    records = new Record[recordBytes.length];
    for (int i = 0; i < records.length; i++) {
      records[i] = new Record("test", common, recordBytes[i]);
    }
  }

  @Test
  public void testFloorFunc() {
    FunctionImpl.FloorFunction func = new FunctionImpl.FloorFunction();
    ConstantImpl constant = new ConstantImpl(1.5d, DataType.Type.DOUBLE.getValue());
    assertEquals(func.evaluate(tableSchemas, records, parms, Collections.singletonList((ExpressionImpl)constant)), 1d);
  }

  @Test
  public void testAbsFunc() {
    FunctionImpl.AbsFunction func = new FunctionImpl.AbsFunction();
    ConstantImpl constant = new ConstantImpl(-1.5d, DataType.Type.DOUBLE.getValue());
    assertEquals(func.evaluate(tableSchemas, records, parms, Collections.singletonList((ExpressionImpl)constant)), 1.5d);
  }

  @Test
  public void testStrFunc() {
    FunctionImpl.StrFunction func = new FunctionImpl.StrFunction();
    ConstantImpl constant = new ConstantImpl(-1.5d, DataType.Type.DOUBLE.getValue());
    assertEquals(func.evaluate(tableSchemas, records, parms, Collections.singletonList((ExpressionImpl)constant)), "-1.5");
  }

  @Test
  public void testAvgFunc() {
    FunctionImpl.AvgFunction func = new FunctionImpl.AvgFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    ColumnImpl column = new ColumnImpl(null, null, tableSchemas[0].getName(), "field1", null);
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    expressions.add(column);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 150d);
  }

  @Test
  public void testMaxFunc() {
    FunctionImpl.MaxFunction func = new FunctionImpl.MaxFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    ColumnImpl column = new ColumnImpl(null, null, tableSchemas[0].getName(), "field1", null);
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    expressions.add(column);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 200d);
  }

  @Test
  public void testMaxTimestampFunc() {
    FunctionImpl.MaxTimestampFunction func = new FunctionImpl.MaxTimestampFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(1000), DataType.Type.TIMESTAMP.getValue());
    ConstantImpl constant2 = new ConstantImpl(new Timestamp(2000), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), new Timestamp(2000));
  }

  @Test
  public void testSumFunc() {
    FunctionImpl.SumFunction func = new FunctionImpl.SumFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    ColumnImpl column = new ColumnImpl(null, null, tableSchemas[0].getName(), "field1", null);
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    expressions.add(column);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 300d);
  }

  @Test
  public void testMinFunc() {
    FunctionImpl.MinFunction func = new FunctionImpl.MinFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    ColumnImpl column = new ColumnImpl(null, null, tableSchemas[0].getName(), "field1", null);
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    expressions.add(column);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 100d);
  }

  @Test
  public void testMinTimestampFunc() {
    FunctionImpl.MinTimestampFunction func = new FunctionImpl.MinTimestampFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(1000), DataType.Type.TIMESTAMP.getValue());
    ConstantImpl constant2 = new ConstantImpl(new Timestamp(2000), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), new Timestamp(1000));
  }

  @Test
  public void testBitShiftLeftFunc() {
    FunctionImpl.BitShiftLeftFunction func = new FunctionImpl.BitShiftLeftFunction();
    ConstantImpl constant1 = new ConstantImpl(8, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(1, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 16L);
  }

  @Test
  public void testBitShiftRightFunc() {
    FunctionImpl.BitShiftRightFunction func = new FunctionImpl.BitShiftRightFunction();
    ConstantImpl constant1 = new ConstantImpl(8, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(1, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 4L);
  }

  @Test
  public void testBitAndFunc() {
    FunctionImpl.BitAndFunction func = new FunctionImpl.BitAndFunction();
    ConstantImpl constant1 = new ConstantImpl(8, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(12, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 8L);
  }

  @Test
  public void testBitNotFunc() {
    FunctionImpl.BitNotFunction func = new FunctionImpl.BitNotFunction();
    ConstantImpl constant1 = new ConstantImpl(2, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), -3L);
  }

  @Test
  public void testBitOrFunc() {
    FunctionImpl.BitOrFunction func = new FunctionImpl.BitOrFunction();
    ConstantImpl constant1 = new ConstantImpl(8, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(4, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 12L);
  }

  @Test
  public void testBitXOrFunc() {
    FunctionImpl.BitXOrFunction func = new FunctionImpl.BitXOrFunction();
    ConstantImpl constant1 = new ConstantImpl(1, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(2, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3L);
  }

  @Test
  public void testCoalesceFunc() {
    FunctionImpl.CoalesceFunction func = new FunctionImpl.CoalesceFunction();
    ConstantImpl constant1 = new ConstantImpl(1L, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(2L, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 1L);
  }

  @Test
  public void testCharLengthFunc() throws UnsupportedEncodingException {
    FunctionImpl.CharLengthFunction func = new FunctionImpl.CharLengthFunction();
    char[] chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);
    ConstantImpl constant1 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3);
  }

  @Test
  public void testConcatFunc() throws UnsupportedEncodingException {
    FunctionImpl.ConcatFunction func = new FunctionImpl.ConcatFunction();
    char[] chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);
    ConstantImpl constant1 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    chars = new char["456".length()];
    "456".getChars(0, "456".length(), chars, 0);
    ConstantImpl constant2 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "123456");
  }

  @Test
  public void testNowFunc() {
    FunctionImpl.NowFunction func = new FunctionImpl.NowFunction();
    Timestamp begin = new Timestamp(System.currentTimeMillis());
    Timestamp during = (Timestamp) func.evaluate(tableSchemas, records, parms, new ArrayList<ExpressionImpl>());
    Timestamp after = new Timestamp(System.currentTimeMillis());
    assertTrue(begin.getTime() <= during.getTime());
    assertTrue(during.getTime() <= after.getTime());
  }

  @Test
  public void testDateAddFunc() {
    FunctionImpl.DateAddFunction func = new FunctionImpl.DateAddFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(1000), DataType.Type.TIMESTAMP.getValue());
    ConstantImpl constant2 = new ConstantImpl(2000, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), new Timestamp(3000));
  }

  @Test
  public void testDayFunc() {
    FunctionImpl.DayFunction func = new FunctionImpl.DayFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 4);
  }

  @Test
  public void testDayOfWeekFunc() {
    FunctionImpl.DayOfWeekFunction func = new FunctionImpl.DayOfWeekFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3);
  }

  @Test
  public void testDayOfYearFunc() {
    FunctionImpl.DayOfYearFunction func = new FunctionImpl.DayOfYearFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 155);
  }

  @Test
  public void testMinuteFunc() {
    FunctionImpl.MinuteFunction func = new FunctionImpl.MinuteFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 2);
  }

  @Test
  public void testMonthFunc() {
    FunctionImpl.MonthFunction func = new FunctionImpl.MonthFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 5);
  }

  @Test
  public void testSecondFunc() {
    FunctionImpl.SecondFunction func = new FunctionImpl.SecondFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 1);
  }

  @Test
  public void testHourFunc() {
    FunctionImpl.HourFunction func = new FunctionImpl.HourFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3);
  }

  @Test
  public void testWeekOfMonthFunc() {
    FunctionImpl.WeekOfMonthFunction func = new FunctionImpl.WeekOfMonthFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 2);
  }

  @Test
  public void testWeekOfYearFunc() {
    FunctionImpl.WeekOfYearFunction func = new FunctionImpl.WeekOfYearFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 23);
  }

  @Test
  public void testYearFunc() {
    FunctionImpl.YearFunction func = new FunctionImpl.YearFunction();
    ConstantImpl constant1 = new ConstantImpl(new Timestamp(2018, 5, 4, 3, 2, 1, 0), DataType.Type.TIMESTAMP.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3918);
  }

  @Test
  public void testPowerFunc() {
    FunctionImpl.PowerFunction func = new FunctionImpl.PowerFunction();
    ConstantImpl constant1 = new ConstantImpl(2, DataType.Type.BIGINT.getValue());
    ConstantImpl constant2 = new ConstantImpl(2, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 4d);
  }

  @Test
  public void testHexFunc() throws UnsupportedEncodingException {
    FunctionImpl.HexFunction func = new FunctionImpl.HexFunction();
    char[] chars = new char["2".length()];
    "2".getChars(0, "2".length(), chars, 0);
    ConstantImpl constant1 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "32");
  }

  @Test
  public void testLogFunc() {
    FunctionImpl.LogFunction func = new FunctionImpl.LogFunction();
    ConstantImpl constant1 = new ConstantImpl(10, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 2.302585092994046d);
  }

  @Test
  public void testLog10Func() {
    FunctionImpl.Log10Function func = new FunctionImpl.Log10Function();
    ConstantImpl constant1 = new ConstantImpl(10, DataType.Type.BIGINT.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 1.0d);
  }

  @Test
  public void testModFunc() {
    FunctionImpl.ModFunction func = new FunctionImpl.ModFunction();
    ConstantImpl constant = new ConstantImpl(2d, DataType.Type.DOUBLE.getValue());
    ColumnImpl column = new ColumnImpl(null, null, tableSchemas[0].getName(), "field1", null);
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(column);
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 0.0d);
  }

  @Test
  public void testLowerFunc() throws UnsupportedEncodingException {
    FunctionImpl.LowerFunction func = new FunctionImpl.LowerFunction();
    char[] chars = new char["Mom".length()];
    "Mom".getChars(0, chars.length, chars, 0);

    ConstantImpl constant = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "mom");
  }

  @Test
  public void testUpperFunc() throws UnsupportedEncodingException {
    FunctionImpl.UpperFunction func = new FunctionImpl.UpperFunction();
    char[] chars = new char["Mom".length()];
    "Mom".getChars(0, chars.length, chars, 0);
    ConstantImpl constant = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "MOM");
  }

  @Test
  public void testIndexOfFunc() throws UnsupportedEncodingException {
    FunctionImpl.IndexOfFunction func = new FunctionImpl.IndexOfFunction();
    char[] chars = new char["Mom".length()];
    "Mom".getChars(0, chars.length, chars, 0);
    ConstantImpl constant1 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    chars = new char["o".length()];
    "o".getChars(0, chars.length, chars, 0);
    ConstantImpl constant2 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 1);
  }

  @Test
  public void testReplaceFunc() throws UnsupportedEncodingException {
    FunctionImpl.ReplaceFunction func = new FunctionImpl.ReplaceFunction();
    char[] chars = new char["Mom".length()];
    "Mom".getChars(0, chars.length, chars, 0);
    ConstantImpl constant1 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    chars = new char["o".length()];
    "o".getChars(0, chars.length, chars, 0);
    ConstantImpl constant2 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    chars = new char["i".length()];
    "i".getChars(0, chars.length, chars, 0);
    ConstantImpl constant3 = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant1);
    expressions.add(constant2);
    expressions.add(constant3);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "Mim");
  }

  @Test
  public void testRoundFunc() {
    FunctionImpl.RoundFunction func = new FunctionImpl.RoundFunction();
    ConstantImpl constant = new ConstantImpl(2.4d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 2L);
  }

  @Test
  public void testPiFunc() {
    FunctionImpl.PiFunction func = new FunctionImpl.PiFunction();
    List<ExpressionImpl> expressions = new ArrayList<>();
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 3.141592653589793d);
  }

  @Test
  public void testSqrtFunc() {
    FunctionImpl.SqrtFunction func = new FunctionImpl.SqrtFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 10d);
  }

  @Test
  public void testTanFunc() {
    FunctionImpl.TanFunction func = new FunctionImpl.TanFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), -0.5872139151569291d);
  }

  @Test
  public void testCosFunc() {
    FunctionImpl.CosFunction func = new FunctionImpl.CosFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 0.8623188722876839d);
  }

  @Test
  public void testSinFunc() {
    FunctionImpl.SinFunction func = new FunctionImpl.SinFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), -0.5063656411097588d);
  }

  @Test
  public void testCotFunc() {
    FunctionImpl.CotFunction func = new FunctionImpl.CotFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), -1.702956919426469d);
  }

  @Test
  public void testTrimFunc() throws UnsupportedEncodingException {
    FunctionImpl.TrimFunction func = new FunctionImpl.TrimFunction();
    char[] chars = new char[" test\t".length()];
    " test\t".getChars(0, chars.length, chars, 0);

    ConstantImpl constant = new ConstantImpl(chars, DataType.Type.VARCHAR.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), "test");
  }

  @Test
  public void testRadiansFunc() {
    FunctionImpl.RadiansFunction func = new FunctionImpl.RadiansFunction();
    ConstantImpl constant = new ConstantImpl(100d, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), 1.7453292519943295d);
  }

  @Test
  public void testIsNullFunc() {
    FunctionImpl.IsNullFunction func = new FunctionImpl.IsNullFunction();
    ConstantImpl constant = new ConstantImpl(null, DataType.Type.DOUBLE.getValue());
    List<ExpressionImpl> expressions = new ArrayList<>();
    expressions.add(constant);
    assertEquals(func.evaluate(tableSchemas, records, parms, expressions), true);
  }
}
