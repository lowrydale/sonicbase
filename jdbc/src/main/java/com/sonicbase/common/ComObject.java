package com.sonicbase.common;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.giraph.utils.Varint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

import static com.sonicbase.common.ComObject.Type.*;

@ExcludeRename
public class ComObject {

  public static final String UTF_8_STR = "utf-8";
  static Int2ObjectOpenHashMap<DynamicType> typesByTag = new Int2ObjectOpenHashMap<>();

  public static class DynamicType {
    int tag;

    public DynamicType(int tag) {
      this.tag = tag;
    }
  }

  public enum Type {
    LONG_TYPE(0),
    INT_TYPE(1),
    STRING_TYPE(2),
    BOOLEAN_TYPE(3),
    BYTE_ARRAY_TYPE(4),
    ARRAY_TYPE(5),
    OBJECT_TYPE(6),
    TINY_INT_TYPE(7),
    SMALL_INT_TYPE(8),
    FLOAT_TYPE(9),
    DOUBLE_TYPE(10),
    BIG_DECIMAL_TYPE(11),
    DATE_TYPE(12),
    TIME_TYPE(13),
    TIME_STAMP_TYPE(14),
    SHORT_TYPE(15);

    final int tag;

    Type(int tag) {
      this.tag = tag;
      typesByTag.put(tag, new DynamicType(tag));
    }
  }

  static Int2ObjectOpenHashMap<DynamicTag> tagsByTag = new Int2ObjectOpenHashMap<>();

  public static class DynamicTag {
    private final DynamicType type;
    private final Tag tagEnum;

    public DynamicTag(Tag tagEnum, DynamicType type) {
      this.tagEnum = tagEnum;
      this.type = type;
    }
  }

  public enum Tag {
    SERIALIZATION_VERSION(1, SHORT_TYPE),
    TABLE_NAME(2, STRING_TYPE),
    INDEX_NAME(3, STRING_TYPE),
    ID(4, LONG_TYPE),
    IS_EXCPLICITE_TRANS(5, BOOLEAN_TYPE),
    TRANSACTION_ID(6, LONG_TYPE),
    RECORD_LENGTH(7, INT_TYPE),
    RECORD_BYTES(8, BYTE_ARRAY_TYPE),
    KEY_LENGTH(9, INT_TYPE),
    KEY_BYTES(10, BYTE_ARRAY_TYPE),
    IS_COMMITTING(11, BOOLEAN_TYPE),
    PRIMARY_KEY_BYTES(12, BYTE_ARRAY_TYPE),
    BYTES(13, BYTE_ARRAY_TYPE),
    LEGACY_EXPRESSION(14, BYTE_ARRAY_TYPE),
    PARMS(15, BYTE_ARRAY_TYPE),
    COUNT_COLUMN(16, STRING_TYPE),
    COUNT_TABLE_NAME(17, STRING_TYPE),
    LEFT_OPERATOR(18, INT_TYPE),
    COLUMN_OFFSETS(19, ARRAY_TYPE),
    KEY_COUNT(20, INT_TYPE),
    SINGLE_VALUE(21, BOOLEAN_TYPE),
    KEYS(22, ARRAY_TYPE),
    OFFSET(23, INT_TYPE),
    LONG_KEY(24, LONG_TYPE),
    RECORDS(25, ARRAY_TYPE),
    RET_KEYS(26, ARRAY_TYPE),
    SCHEMA_VERSION(27, INT_TYPE),
    PREPARED_ID(28, LONG_TYPE),
    IS_PREPARED(29, BOOLEAN_TYPE),
    COUNT(30, INT_TYPE),
    VIEW_VERSION(31, LONG_TYPE),
    DB_NAME(32, STRING_TYPE),
    METHOD(33, STRING_TYPE),
    TABLE_ID(34, INT_TYPE),
    INDEX_ID(35, INT_TYPE),
    FORCE_SELECT_ON_SERVER(36, BOOLEAN_TYPE),
    EVALUATE_EXPRESSION(37, BOOLEAN_TYPE),
    ORDER_BY_EXPRESSIONS(38, ARRAY_TYPE),
    LEFT_KEY(39, BYTE_ARRAY_TYPE),
    ORIGINAL_LEFT_KEY(40, BYTE_ARRAY_TYPE),
    RIGHT_KEY(41, BYTE_ARRAY_TYPE),
    ORIGINAL_RIGHT_KEY(42, BYTE_ARRAY_TYPE),
    RIGHT_OPERATOR(43, INT_TYPE),
    COUNTERS(44, ARRAY_TYPE),
    LEGACY_GROUP_CONTEXT(45, BYTE_ARRAY_TYPE),
    LEGACY_SELECT_STATEMENT(46, BYTE_ARRAY_TYPE),
    TABLE_RECORDS(47, ARRAY_TYPE),
    LEGACY_COUNTER(48, BYTE_ARRAY_TYPE),
    SLAVE(49, BOOLEAN_TYPE),
    MASTER_SLAVE(50, STRING_TYPE),
    FINISHED(51, BOOLEAN_TYPE),
    SHARD(52, INT_TYPE),
    OFFSETS(53, ARRAY_TYPE),
    SIZE(54, LONG_TYPE),
    TABLES(55, ARRAY_TYPE),
    INDICES(56, ARRAY_TYPE),
    FORCE(57, BOOLEAN_TYPE),
    PRIMARY_KEY_INDEX_NAME(58, STRING_TYPE),
    INSERT_OBJECT(59, OBJECT_TYPE),
    INSERT_OBJECTS(60, ARRAY_TYPE),
    PHASE(61, STRING_TYPE),
    SCHEMA_BYTES(62, BYTE_ARRAY_TYPE),
    CREATE_TABLE_STATEMENT(63, BYTE_ARRAY_TYPE),
    COLUMN_NAME(64, STRING_TYPE),
    DATA_TYPE(65, STRING_TYPE),
    IS_UNIQUE(66, BOOLEAN_TYPE),
    FIELDS_STR(67, STRING_TYPE),
    RESULT_SET_ID(68, LONG_TYPE),
    COUNT_LONG(69, LONG_TYPE),
    REQUESTED_MASTER_SHARD(70, INT_TYPE),
    REQUESTED_MASTER_REPLICA(71, INT_TYPE),
    SELECTED_MASTE_REPLICA(72, INT_TYPE),
    ELECTED_MASTER(73, INT_TYPE),
    REPLICA(74, INT_TYPE),
    DIRECTORY(75, STRING_TYPE),
    SUB_DIRECTORY(76, STRING_TYPE),
    BUCKET(77, STRING_TYPE),
    PREFIX(78, STRING_TYPE),
    IS_COMPLETE(79, BOOLEAN_TYPE),
    SHARED(80, BOOLEAN_TYPE),
    MAX_BACKUP_COUNT(81, INT_TYPE),
    FILENAME(82, STRING_TYPE),
    FILE_CONTENT(83, STRING_TYPE),
    IS_CLIENT(84, BOOLEAN_TYPE),
    HOST(85, STRING_TYPE),
    MESSAGE(86, STRING_TYPE),
    EXCEPTION(87, STRING_TYPE),
    RES_GIG(88, DOUBLE_TYPE),
    CPU(89, DOUBLE_TYPE),
    JAVA_MEM_MIN(90, DOUBLE_TYPE),
    JAVA_MEM_MAX(91, DOUBLE_TYPE),
    AVG_REC_RATE(92, DOUBLE_TYPE),
    AVG_TRANS_RATE(93, DOUBLE_TYPE),
    DISK_AVAIL(94, STRING_TYPE),
    PORT(95, INT_TYPE),
    DB_NAMES(96, ARRAY_TYPE),
    SERVERS_CONFIG(97, BYTE_ARRAY_TYPE),
    STATUS(98, STRING_TYPE),
    SEQUENCE_NUMBER(99, LONG_TYPE),
    CONFIG_BYTES(100, BYTE_ARRAY_TYPE),
    HIGHEST_ID(101, LONG_TYPE),
    NEXT_ID(102, LONG_TYPE),
    MAX_ID(103, LONG_TYPE),
    BINARY_FILE_CONTENT(104, BYTE_ARRAY_TYPE),
    TYPE(105, STRING_TYPE),
    FILENAMES(106, ARRAY_TYPE),
    HAVE_PRO_LICENSE(107, BOOLEAN_TYPE),
    FILES(108, ARRAY_TYPE),
    SEQUENCE_0(109, LONG_TYPE),
    SEQUENCE_1(110, LONG_TYPE),
    PERCENT_COMPLETE(111, DOUBLE_TYPE),
    STAGE(112, STRING_TYPE),
    ERROR(113, BOOLEAN_TYPE),
    COMMAND(114, STRING_TYPE),
    IN_COMPLIANCE(115, BOOLEAN_TYPE),
    DISABLE_NOW(116, BOOLEAN_TYPE),
    CORE_COUNT(117, INT_TYPE),
    STATE(118, STRING_TYPE),
    SHARDS(119, ARRAY_TYPE),
    DISABLE_DATE(120, STRING_TYPE),
    MULTIPLE_LICENSE_SERVERS(121, BOOLEAN_TYPE),
    MIN_KEY(122, BYTE_ARRAY_TYPE),
    MAX_KEY(123, BYTE_ARRAY_TYPE),
    DRIVER_NAME(124, STRING_TYPE),
    USER(125, STRING_TYPE),
    PASSWORD(126, STRING_TYPE),
    CONNECT_STRING(127, STRING_TYPE),
    OFFSET_LONG(128, LONG_TYPE),
    LIMIT_LONG(129, LONG_TYPE),
    EXPECTED_COUNT(130, LONG_TYPE),
    PROGRESS_OBJECT(131, OBJECT_TYPE),
    PROGRESS_ARRAY(132, ARRAY_TYPE),
    CURR_OFFSET(133, LONG_TYPE),
    ACCEPTED(134, BOOLEAN_TYPE),
    STATUSES(135, ARRAY_TYPE),
    PRE_POCESS_COUNT_PROCESSED(136, LONG_TYPE),
    PRE_PROCESS_EXPECTED_COUNT(137, LONG_TYPE),
    PRE_PROCESS_FINISHED(138, BOOLEAN_TYPE),
    SHOULD_PROCESS(139, BOOLEAN_TYPE),
    PRE_PROCESS_EXCEPTION(140, STRING_TYPE),
    NEXT_KEY(149, BYTE_ARRAY_TYPE),
    LOWER_KEY(150, BYTE_ARRAY_TYPE),
    WHERE_CLAUSE(151, STRING_TYPE),
    KEY_RECORD_BYTES(152, BYTE_ARRAY_TYPE),
    KEY_RECORDS(153, ARRAY_TYPE),
    HEADER(154, OBJECT_TYPE),
    REPLICATION_MASTER(155, INT_TYPE),
    SELECT_STATEMENTS(156, ARRAY_TYPE),
    OPERATIONS(157, ARRAY_TYPE),
    SERVER_SELECT_PAGE_NUMBER(158, LONG_TYPE),
    IGNORE(159, BOOLEAN_TYPE),
    COLUMNS(160, ARRAY_TYPE),
    SELECT(161, BYTE_ARRAY_TYPE),
    ALIAS(162, STRING_TYPE),
    FUNCTION(163, STRING_TYPE),
    IS_PROBE(164, BOOLEAN_TYPE),
    SOURCE_SIZE(165, LONG_TYPE),
    DEST_SIZE(166, LONG_TYPE),
    CURR_REQUEST_IS_MASTER(167, BOOLEAN_TYPE),
    SEQUENCE_0_OVERRIDE(168, LONG_TYPE),
    SEQUENCE_1_OVERRIDE(169, LONG_TYPE),
    SEQUENCE_2_OVERRIDE(170, SHORT_TYPE),
    MESSAGES(171, ARRAY_TYPE),
    IS_STARTED(172, BOOLEAN_TYPE),
    SQL(173, STRING_TYPE),
    FIELD_NAME(174, STRING_TYPE),
    STRING_VALUE(175, STRING_TYPE),
    FIELDS(176, ARRAY_TYPE),
    LONG_VALUE(177, LONG_TYPE),
    INT_VALUE(178, INT_TYPE),
    BOOLEAN_VALUE(179, BOOLEAN_TYPE),
    TIME_VALUE(180, TIME_TYPE),
    DATE_VALUE(181, DATE_TYPE),
    TIMESTAMP_VALUE(182, TIME_STAMP_TYPE),
    FLOAT_VALUE(183, FLOAT_TYPE),
    DOUBLE_VALUE(184, DOUBLE_TYPE),
    BIG_DECIMAL_VALUE(185, BIG_DECIMAL_TYPE),
    BYTE_VALUE(186, TINY_INT_TYPE),
    SHORT_VALUE(187, SMALL_INT_TYPE),
    BYTE_ARRAY_VALUE(188, BYTE_ARRAY_TYPE),
    BEGIN_MILLIS(189, LONG_TYPE),
    DURATION(190, LONG_TYPE),
    DURATION_DOUBLE(191, DOUBLE_TYPE),
    HISTOGRAM_SNAPSHOT(192, ARRAY_TYPE),
    LAT_AVG(193, DOUBLE_TYPE),
    LAT_75(194, DOUBLE_TYPE),
    LAT_95(195, DOUBLE_TYPE),
    LAT_99(196, DOUBLE_TYPE),
    LAT_999(197, DOUBLE_TYPE),
    LAT_MAX(198, DOUBLE_TYPE),
    LATENCIES_BYTES(199, BYTE_ARRAY_TYPE),
    COUNT_RETURNED(200, LONG_TYPE),
    ORIGINAL_OFFSET(201, INT_TYPE),
    BATCH_RESPONSES(202, ARRAY_TYPE),
    INT_STATUS(203, INT_TYPE),
    ORIGINAL_IGNORE(204, BOOLEAN_TYPE),
    DATABASES(205, ARRAY_TYPE),
    HAS_DISCREPANCY(206, BOOLEAN_TYPE),
    TABLE_SCHEMA(207, BYTE_ARRAY_TYPE),
    INDEX_SCHEMA(208, BYTE_ARRAY_TYPE);

    public final int tag;

    Tag(int tag, Type type) {
      this.tag = tag;
      if (tagsByTag.put(tag, new DynamicTag(this, typesByTag.get(type.tag))) != null) {
        throw new DatabaseException("Duplicate tag in ComObject: id=" + tag);
      }
    }
  }

  public static Tag getTag(int tag) {
    return tagsByTag.get(tag).tagEnum;
  }

  public ComObject() {
    put(ComObject.Tag.SERIALIZATION_VERSION, (short) DatabaseClient.SERIALIZATION_VERSION);
  }

  public ComObject(byte[] bytes) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    deserialize(in);
  }

  public ComObject(DataInputStream in) {
    deserialize(in);
  }

  private Int2ObjectOpenHashMap map = new Int2ObjectOpenHashMap();

  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (Object entry : map.entrySet()) {
      Int2ObjectOpenHashMap.Entry<Object> entryObj = (Int2ObjectOpenHashMap.Entry<Object>) entry;
      builder.append("[").append(ComObject.getTag( entryObj.getIntKey()).name()).append("=").append(entryObj.getValue()).append("]");
    }
    return builder.toString();
  }

  public Map<Integer, Object> getMap() {
    return map;
  }

  public boolean containsTag(Tag tag) {
    return map.containsKey(tag.tag);
  }

  public void put(Tag tag, ComObject value) {
    map.put(tag.tag, value);
  }

  public void put(Tag tag, long value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, int value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, short value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, float value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, double value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, String value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, boolean value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, byte[] bytes) {
    map.put(tag.tag, (Object)bytes);
  }

  public void put(Tag tag, Time time) {
    map.put(tag.tag, time);
  }

  public void put(Tag tag, Timestamp timestamp) {
    map.put(tag.tag, timestamp);
  }

  public void put(Tag tag, Date date) {
    map.put(tag.tag, date);
  }

  public void put(Tag tag, BigDecimal value) {
    map.put(tag.tag, value);
  }

  public void put(Tag tag, byte value) {
    map.put(tag.tag, (Object)value);
  }


  public Long getLong(Tag tag) {
    return (Long)map.get(tag.tag);
  }

  public Short getShort(Tag tag) {
    return (Short)map.get(tag.tag);
  }

  public Integer getInt(Tag tag) {
    return (Integer)map.get(tag.tag);
  }

  public Float getFloat(Tag tag) {
    return (Float)map.get(tag.tag);
  }

  public Double getDouble(Tag tag) {
    return (Double)map.get(tag.tag);
  }

  public String getString(Tag tag) {
    return (String)map.get(tag.tag);
  }

  public Boolean getBoolean(Tag tag) {
    return (Boolean)map.get(tag.tag);
  }

  public byte[] getByteArray(Tag tag) {
    return (byte[])map.get(tag.tag);
  }

  public ComObject getObject(Tag tag) {
    return (ComObject)map.get(tag.tag);
  }

  public Time getTime(Tag tag) {
    return (Time)map.get(tag.tag);
  }

  public Timestamp getTimestamp(Tag tag) {
    return (Timestamp)map.get(tag.tag);
  }

  public Date getDate(Tag tag) {
    return (Date)map.get(tag.tag);
  }

  public BigDecimal getBigDecimal(Tag tag) {
    return (BigDecimal)map.get(tag.tag);
  }

  public byte getByte(Tag tag) {
    return (byte)map.get(tag.tag);
  }

  public ComObject putObject(Tag tag) {
    ComObject cobj = new ComObject();
    cobj.remove(Tag.SERIALIZATION_VERSION);
    map.put(tag.tag, cobj);
    return cobj;
  }

  public ComArray putArray(Tag tag, Type nestedType) {
    ComArray ret = new ComArray(nestedType);
    map.put(tag.tag, ret);
    return ret;
  }

  public ComArray putArray(Tag tag, ComArray newArray) {
    map.put(tag.tag, newArray);
    return newArray;
  }



  public ComArray getArray(Tag tag) {
    return (ComArray)map.get(tag.tag);
  }

  public void remove(Tag tag) {
    map.remove(tag.tag);
  }

  public void deserialize(byte[] bytes) {
    deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public void deserialize(DataInputStream in) {
    try {
      map.clear();
      int count = (int) Varint.readSignedVarLong(in);
      for (int i = 0; i < count; i++) {
        int tag = (int) Varint.readSignedVarLong(in);
        int typeTag = (int)Varint.readSignedVarLong(in);
        DynamicType type = typesByTag.get(typeTag);

        Object value = null;
        if (type.tag == INT_TYPE.tag) {
          value = (int)Varint.readSignedVarLong(in);
        }
        else if (type.tag == SHORT_TYPE.tag) {
          value = (short)Varint.readSignedVarLong(in);
        }
        else if (type.tag == LONG_TYPE.tag) {
          value = Varint.readSignedVarLong(in);
        }
        else if (type.tag == STRING_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          value = new String(bytes, UTF_8_STR);
        }
        else if (type.tag == BOOLEAN_TYPE.tag) {
          value = in.readBoolean();
        }
        else if (type.tag == BYTE_ARRAY_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          value = bytes;
        }
        else if (type.tag == ARRAY_TYPE.tag) {
          value = new ComArray(in);
        }
        else if (type.tag == OBJECT_TYPE.tag) {
          value = new ComObject(in);
        }
        else if (type.tag == TINY_INT_TYPE.tag) {
          value = in.read();
        }
        else if (type.tag == SMALL_INT_TYPE.tag) {
          value = in.readShort();
        }
        else if (type.tag == FLOAT_TYPE.tag) {
          value = in.readFloat();
        }
        else if (type.tag == DOUBLE_TYPE.tag) {
          value = in.readDouble();
        }
        else if (type.tag == BIG_DECIMAL_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          String str = new String(bytes, UTF_8_STR);
          value = new java.math.BigDecimal(str);
        }
        else if (type.tag == DATE_TYPE.tag) {
          java.sql.Date date = new java.sql.Date(Varint.readSignedVarLong(in));
          value = date;
        }
        else if (type.tag == TIME_TYPE.tag) {
          java.sql.Time time = new java.sql.Time(Varint.readSignedVarLong(in));
          value = time;
        }
        else if (type.tag == TIME_STAMP_TYPE.tag) {
          java.sql.Timestamp timestamp = Timestamp.valueOf(in.readUTF());
          value = timestamp;
        }

        else {
          throw new DatabaseException("Don't know how to deserialize type: type=" + type.tag);
        }
        map.put(tag, value);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] serialize() {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      Varint.writeSignedVarLong(map.size(), out);
      Iterator<Int2ObjectMap.Entry<Object>> iterator = map.int2ObjectEntrySet().fastIterator();
      while (iterator.hasNext()) {
        doSerialize(out, iterator);
      }

      out.close();
      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doSerialize(DataOutputStream out, Iterator<Int2ObjectMap.Entry<Object>> iterator) {
    int tag = -1;
    try {
      Int2ObjectMap.Entry<Object> entry = iterator.next();
      tag = entry.getIntKey();
      Object value = entry.getValue();
      DynamicTag tagObj = tagsByTag.get(tag);
      if (tagObj == null) {
        throw new DatabaseException("Tag not defined: tag=" + tag);
      }
      Varint.writeSignedVarLong(tag, out);
      Varint.writeSignedVarLong(tagObj.type.tag, out);
      if (tagObj.type.tag == INT_TYPE.tag) {
        if (value instanceof Integer) {
          Varint.writeSignedVarLong((Integer) value, out);
        }
        else if (value instanceof Long) {
          Varint.writeSignedVarLong((Long) value, out);
        }
        else {
          throw new DatabaseException("Invalid type: class=" + value.getClass());
        }
      }
      else if (tagObj.type.tag == SHORT_TYPE.tag) {
        Varint.writeSignedVarLong((Short) value, out);
      }
      else if (tagObj.type.tag == LONG_TYPE.tag) {
        if (value instanceof Integer) {
          Varint.writeSignedVarLong((Integer) value, out);
        }
        else if (value instanceof Long){
          Varint.writeSignedVarLong((Long) value, out);
        }
        else {
          throw new DatabaseException("Invalid type: class=" + value.getClass());
        }
      }
      else if (tagObj.type.tag == STRING_TYPE.tag) {
        byte[] bytes = ((String) value).getBytes(UTF_8_STR);
        Varint.writeSignedVarLong(bytes.length, out);
        out.write(bytes);
      }
      else if (tagObj.type.tag == BOOLEAN_TYPE.tag) {
        out.writeBoolean((Boolean) value);
      }
      else if (tagObj.type.tag == BYTE_ARRAY_TYPE.tag) {
        byte[] bytes = (byte[]) value;
        Varint.writeSignedVarLong(bytes.length, out);
        out.write(bytes);
      }
      else if (tagObj.type.tag == ARRAY_TYPE.tag) {
        ((ComArray) value).serialize(out);
      }
      else if (tagObj.type.tag == OBJECT_TYPE.tag) {
        out.write(((ComObject) value).serialize());
      }
      else if (tagObj.type.tag == TINY_INT_TYPE.tag) {
        out.write((byte) value);
      }
      else if (tagObj.type.tag == SMALL_INT_TYPE.tag) {
        out.writeShort((short) value);
      }
      else if (tagObj.type.tag == FLOAT_TYPE.tag) {
        out.writeFloat((float) value);
      }
      else if (tagObj.type.tag == DOUBLE_TYPE.tag) {
        out.writeDouble((double) value);
      }
      else if (tagObj.type.tag == BIG_DECIMAL_TYPE.tag) {
        byte[] bytes = ((BigDecimal) value).toPlainString().getBytes(UTF_8_STR);
        Varint.writeSignedVarLong(bytes.length, out);
        out.write(bytes);
      }
      else if (tagObj.type.tag == DATE_TYPE.tag) {
        Varint.writeSignedVarLong(((Date) value).getTime(), out);
      }
      else if (tagObj.type.tag == TIME_TYPE.tag) {
        Varint.writeSignedVarLong(((Time) value).getTime(), out);
      }
      else if (tagObj.type.tag == TIME_STAMP_TYPE.tag) {
        out.writeUTF(((Timestamp) value).toString());
      }
    }
    catch (Exception e) {
      throw new DatabaseException("Error serializing field: tag=" + tag, e);
    }
  }
}
