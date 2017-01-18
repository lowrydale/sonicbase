package com.lowryengineering.database.jdbcdriver;

import com.lowryengineering.database.query.impl.ResultSetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Oct 7, 2011
 * Time: 3:12:03 PM
 */
public class ResultSetProxy implements java.sql.ResultSet {

  private static Logger LOGGER = LoggerFactory.getLogger(ResultSetProxy.class);

  private List<ResultSetInfo> resultSets = new ArrayList<ResultSetInfo>();
  private int currResultSetOffset;
  private int currRow = 0;
  private ResultSetImpl resultSet;
  private boolean wasNull = false;

  public enum FieldType {
    BIT("BIT", Types.BIT),
    TINYINT("TINYINT", Types.TINYINT),
    SMALLINT("SMALLINT", Types.TINYINT),
    INTEGER("INTEGER", Types.INTEGER),
    BIGINT("BIGINT", Types.BIGINT),
    FLOAT("FLOAT", Types.FLOAT),
    REAL("REAL", Types.REAL),
    DOUBLE("DOUBLE", Types.DOUBLE),
    NUMERIC("NUMERIC", Types.NUMERIC),
    DECIMAL("DECIMAL", Types.DECIMAL),
    CHAR("CHAR", Types.CHAR),
    VARCHAR("VARCHAR", Types.VARCHAR),
    LONGVARCHAR("LONGVARCHAR", Types.LONGVARCHAR),
    DATE("DATE", Types.DATE),
    TIME("TIME", Types.TIME),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP),
    BINARY("BINARY", Types.BINARY),
    VARBINARY("VARBINARY", Types.VARBINARY),
    LONGVARBINARY("LONGVARBINARY", Types.LONGVARBINARY),
    NULL("NULL", Types.NULL),
    OTHER("OTHER", Types.OTHER),
    JAVA_OBJECT("JAVA_OBJECT", Types.JAVA_OBJECT),
    DISTINCT("DISTINCT", Types.DISTINCT),
    STRUCT("STRUCT", Types.STRUCT),
    ARRAY("ARRAY", Types.ARRAY),
    BLOB("BLOB", Types.BLOB),
    CLOB("CLOB", Types.CLOB),
    REF("REF", Types.REF),
    DATALINK("DATALINK", Types.DATALINK),
    BOOLEAN("BOOLEAN", Types.BOOLEAN),
    ROWID("ROWID", Types.ROWID),
    NCHAR("NCHAR", Types.NCHAR),
    NVARCHAR("NVARCHAR", Types.NVARCHAR),
    LONGNVARCHAR("LONGNVARCHAR", Types.LONGNVARCHAR),
    NCLOB("NCLOB", Types.NCLOB),
    SQLXML("SQLXML", Types.SQLXML),
    BYTEA("BYTEA", Types.BINARY),

    OID("OID", 9000000);

    private String name;
    private int type;

    FieldType(String name, int type) {
      this.name = name;
      this.type = type;
    }
  }

  private static class Table {
    private String name;

    private Table(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

  }

  private static Map<String, Integer> lookupTypeByName = new HashMap<String, Integer>();

  static {
    try {
      for (FieldType field : FieldType.values()) {
        lookupTypeByName.put(field.name.toUpperCase(), field.type);
      }

    }
    catch(Exception t) {
      LOGGER.error("Error initialing ResultSetProxy", t);
    }
  }

  public ResultSetProxy(ConnectionProxy outerConnection, ResultSetImpl ret) {
    this.resultSet = ret;

    resultSets.add(new ResultSetInfo(resultSet));
    synchronized (loadedBlobs) {
      if (null == loadedBlobs.get()) {
        loadedBlobs.set(new ConcurrentHashMap<Long, Blob>());
      }
    }
  }

  private static final ThreadLocal<ConcurrentHashMap<Long, Blob>> loadedBlobs = new ThreadLocal<ConcurrentHashMap<Long, Blob>>();

  public static class ResultSetInfo {
    private ResultSetImpl resultSet;
    private int currRow = 0;
    private int highestIndex;

    public ResultSetInfo(ResultSetImpl resultSet) {
      this.resultSet = resultSet;
    }

  }

  public boolean next() throws SQLException {
    try {
      if (resultSets.size() == 0) {
        return false;
      }
      int startOffset = currResultSetOffset;
      while (true) {
        currResultSetOffset = (currResultSetOffset + 1) % resultSets.size();
        int newHighestResultSet = currResultSetOffset;
        ResultSetInfo curr = resultSets.get(newHighestResultSet);

        boolean afterLast = curr.resultSet != null && curr.resultSet.isAfterLast() && curr.currRow > curr.highestIndex;

        if (!afterLast) {
          if (curr.resultSet == null) {
            return false;
          }
          //this row has not been read, go ahead and buffer it
          if (curr.resultSet.next()) {
            curr.currRow++;
            currRow++;
            curr.highestIndex = Math.max(curr.currRow, curr.highestIndex);
            return true;
          }
        }

        // we've cycled through all the result sets and didn't find any more hits
        if (currResultSetOffset == startOffset) {
          return false;
        }
      }
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean isBeforeFirst() throws SQLException {
    for (ResultSetInfo info : resultSets) {
      if (info.resultSet != null && !info.resultSet.isBeforeFirst()) {
        return false;
      }
    }
    return true;
  }

  public boolean isAfterLast() throws SQLException {
    for (ResultSetInfo info : resultSets) {
      if (info.resultSet != null && !info.resultSet.isAfterLast()) {
        return false;
      }
    }
    return true;
  }

  public boolean isFirst() throws SQLException {
    for (ResultSetInfo resultSet : resultSets) {
      if (resultSet.resultSet != null && !resultSet.resultSet.isFirst()) {
        return false;
      }
    }
    return true;
  }

  public boolean isLast() throws SQLException {
    for (ResultSetInfo info : resultSets) {
      if (info.resultSet != null && (!info.resultSet.isLast() && !info.resultSet.isAfterLast())) {
        return false;
      }
    }
    return true;
  }

  public void beforeFirst() throws SQLException {
    throw new SQLException("not supported");
  }

  public void afterLast() throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean first() throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean last() throws SQLException {
    throw new SQLException("not supported");
  }

  public int getRow() throws SQLException {
    return currRow;
  }

  public boolean absolute(int row) throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean relative(int rows) throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean previous() throws SQLException {
    throw new SQLException("not supported");
  }


  public void close() throws SQLException {
    for (ResultSetInfo info : resultSets) {
      if (info.resultSet != null) {
        try {
          info.resultSet.close();
        }
        catch (Exception e) {
          LOGGER.error("Error closing resultSet", e);
        }
      }
    }
  }

  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  public String getString(int columnIndex) throws SQLException {
    String ret = resultSet.getString(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    Boolean ret = resultSet.getBoolean(columnIndex);
    if (ret == null) {
      wasNull = true;
      return false;
    }
    wasNull = false;
    return ret;
  }

  public byte getByte(int columnIndex) throws SQLException {
    Byte ret = resultSet.getByte(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public short getShort(int columnIndex) throws SQLException {
    Short ret = resultSet.getShort(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public int getInt(int columnIndex) throws SQLException {
    Integer ret = resultSet.getInt(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public long getLong(int columnIndex) throws SQLException {
    Long ret = resultSet.getLong(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public float getFloat(int columnIndex) throws SQLException {
    Float ret = resultSet.getFloat(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public double getDouble(int columnIndex) throws SQLException {
    Double ret = resultSet.getDouble(columnIndex);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    BigDecimal ret = resultSet.getBigDecimal(columnIndex, scale);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    byte[] ret = resultSet.getBytes(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Date getDate(int columnIndex) throws SQLException {
    Date ret = resultSet.getDate(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Time getTime(int columnIndex) throws SQLException {
    Time ret = resultSet.getTime(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Timestamp ret = resultSet.getTimestamp(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public String getString(String columnLabel) throws SQLException {
    try {
      String ret = resultSet.getString(columnLabel);
      if (ret == null) {
        wasNull = true;
        return null;
      }
      wasNull = false;
      return ret;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean getBoolean(String columnLabel) throws SQLException {
    Boolean ret = resultSet.getBoolean(columnLabel);
    if (ret == null) {
      wasNull = true;
      return false;
    }
    wasNull = false;
    return ret;
  }

  public byte getByte(String columnLabel) throws SQLException {
    Byte ret = resultSet.getByte(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public short getShort(String columnLabel) throws SQLException {
    Short ret = resultSet.getShort(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public int getInt(String columnLabel) throws SQLException {
    Integer ret = resultSet.getInt(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public long getLong(String columnLabel) throws SQLException {
      Long ret = resultSet.getLong(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public float getFloat(String columnLabel) throws SQLException {
    Float ret = resultSet.getFloat(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public double getDouble(String columnLabel) throws SQLException {
    Double ret = resultSet.getDouble(columnLabel);
    if (ret == null) {
      wasNull = true;
      return 0;
    }
    wasNull = false;
    return ret;
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    BigDecimal ret = resultSet.getBigDecimal(columnLabel, scale);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    byte[]  ret = resultSet.getBytes(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Date getDate(String columnLabel) throws SQLException {
    Date ret = resultSet.getDate(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Time getTime(String columnLabel) throws SQLException {
    Time ret = resultSet.getTime(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    Timestamp ret = resultSet.getTimestamp(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    InputStream ret = resultSet.getUnicodeStream(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    InputStream ret = resultSet.getBinaryStream(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    InputStream ret = resultSet.getBinaryStream(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }


  public Object getObject(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public Object getObject(String columnLabel) throws SQLException {
    throw new SQLException("Not supported");
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public Reader getCharacterStream(String columnLabel) throws SQLException {
    Reader ret = resultSet.getCharacterStream(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    BigDecimal ret = resultSet.getBigDecimal(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    BigDecimal ret = resultSet.getBigDecimal(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public Blob getBlob(int columnIndex) throws SQLException {
    byte[]ret = resultSet.getBytes(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.Blob(ret);
  }

  public Clob getClob(int columnIndex) throws SQLException {
    String ret = resultSet.getString(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.Clob(ret);
  }

  public Array getArray(int columnIndex) throws SQLException {
    throw new SQLException("Not supported");
  }

  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("not supported");
  }

  public Ref getRef(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public Blob getBlob(String columnLabel) throws SQLException {
    byte[]ret = resultSet.getBytes(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.Blob(ret);
  }

  public Clob getClob(String columnLabel) throws SQLException {
    String ret = resultSet.getString(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.Clob(ret);
  }

  public Array getArray(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
//    if (getCurrentRow() == null) {
//      wasNull = true;
//      return null;
//    }
//    Date ret = getCurrentRow().getDate(columnIndex, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
//      columnLabel = getNameFromAlias(columnLabel, getCurrentRow().getSelectStatement());
//    if (getCurrentRow() == null) {
//      wasNull = true;
//      return null;
//    }
//    Date ret = getCurrentRow().getDate(columnLabel, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
//    if (getCurrentRow() == null) {
//      wasNull = true;
//      return null;
//    }
//    Time ret = getCurrentRow().getTime(columnIndex, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
//      columnLabel = getNameFromAlias(columnLabel, getCurrentRow().getSelectStatement());
//    if (getCurrentRow() == null) {
//      wasNull = true;
//      return null;
//    }
//    Time ret = getCurrentRow().getTime(columnLabel, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
//    Timestamp ret = resultSet.getTimestamp(columnIndex, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
//      columnLabel = getNameFromAlias(columnLabel, getCurrentRow().getSelectStatement());
//    if (getCurrentRow() == null) {
//      wasNull = true;
//      return null;
//    }
//    Timestamp ret = getCurrentRow().getTimestamp(columnLabel, cal);
//    if (ret == null) {
//      wasNull = true;
//      return null;
//    }
//    wasNull = false;
//    return ret;
    return null;
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLException("not supported");
  }

  public URL getURL(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    String ret = resultSet.getString(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.NClob(ret);
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    String ret = resultSet.getString(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new com.lowryengineering.database.query.impl.NClob(ret);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLException("not supported");
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public String getNString(int columnIndex) throws SQLException {
    String ret = resultSet.getString(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public String getNString(String columnLabel) throws SQLException {
    String ret = resultSet.getString(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return ret;
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    String ret = resultSet.getString(columnIndex);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new StringReader(ret);
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    String ret = resultSet.getString(columnLabel);
    if (ret == null) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    return new StringReader(ret);
  }

  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("not supported");
  }

  public void clearWarnings() throws SQLException {
    throw new SQLException("not supported");
  }

  public String getCursorName() throws SQLException {
    throw new SQLException("not supported");
  }

  public ResultSetMetaData getMetaData() throws SQLException {
//    if (resultSetMetadataForBufferedRow != null) {
//      return resultSetMetadataForBufferedRow;
//    }
//
//    if (this.statement instanceof SelectStatement) {
//      SelectStatement selectStatement = (SelectStatement) statement;
//      List<FromTable> fromList = selectStatement.getSelectNode().getFromList();
//      Table table = null;
//      if (resultSets.size() > 0 && resultSets.get(0).resultSet != null) {
//        return resultSets.get(0).resultSet.getMetaData();
//      }
////      else {
////        table = lookupTableByName.get(fromList.get(0).getTableName().toUpperCase()); //todo: support multiple froms
////        if (table == null) {
////          return new MetaDataProxy();
////        }
////      }
//      return new MetaDataProxy();
//      //todo: all branches have same result
//    }
//    else {
//      return new MetaDataProxy();
//    }
    throw new SQLException("not supported");
  }

  public int findColumn(String columnLabel) throws SQLException {
    throw new NotImplementedException();
  }


  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("not supported");
  }

  public int getFetchDirection() throws SQLException {
    throw new SQLException("not supported");
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException("not supported");
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException("not supported");
  }

  public int getType() throws SQLException {
    throw new SQLException("not supported");
  }

  public int getConcurrency() throws SQLException {
    //return resultSets.get(currResultSetOffset).resultSet.getConcurrency();
    return 0;
  }

  public boolean rowUpdated() throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean rowInserted() throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean rowDeleted() throws SQLException {
    throw new SQLException("not supported");
  }

  //todo: all upate methods need to integrate with buffered rows
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void insertRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public void deleteRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public void refreshRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("not supported");
  }

  public void moveToInsertRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("not supported");
  }

  public java.sql.Statement getStatement() throws SQLException {
    //return resultSets.get(currResultSetOffset).resultSet.getStatement();
    return null;
  }

  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLException("not supported");
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLException("not supported");
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException("not supported");
  }

  public int getHoldability() throws SQLException {
    //return resultSets.get(currResultSetOffset).resultSet.getHoldability();
    return 0;
  }

  public boolean isClosed() throws SQLException {
    //return resultSets.get(currResultSetOffset).resultSet.isClosed();
    return false;
  }

  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLException("not supported");
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLException("not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("not supported");
  }
}
