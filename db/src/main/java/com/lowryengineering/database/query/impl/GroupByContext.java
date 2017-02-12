package com.lowryengineering.database.query.impl;

import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.util.DataUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupByContext {
  private List<FieldContext> fieldContexts;
  private Map<String, Map<Object[], GroupCounter>> groupCounters;
  private Map<String, Counter> counterTemplates = new ConcurrentHashMap<>();

  public GroupByContext() {
  }

  public GroupByContext(List<FieldContext> fieldContexts) {
    this.fieldContexts = fieldContexts;
    this.groupCounters = new ConcurrentHashMap<>();//new ConcurrentSkipListMap<>(comparator);
  }

  public static class FieldContext {
    private String fieldName;
    private int fieldOffset;
    private DataType.Type dataType;
    private Comparator comparator;
    public TableSchema tableSchema;

    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    public void setFieldOffset(int fieldOffset) {
      this.fieldOffset = fieldOffset;
    }

    public void setDataType(DataType.Type dataType) {
      this.dataType = dataType;
    }

    public void setComparator(Comparator comparator) {
      this.comparator = comparator;
    }

    public String getFieldName() {
      return fieldName;
    }

    public int getFieldOffset() {
      return fieldOffset;
    }

    public DataType.Type getDataType() {
      return dataType;
    }

    public Comparator getComparator() {
      return comparator;
    }
  }

  public void addGroupContext(Object[] groupValues) {
    for (Map.Entry<String, Counter> entry : counterTemplates.entrySet()) {
      GroupCounter counter = new GroupCounter();
      counter.setGroupValues(groupValues);
      counter.counter = new Counter();
      Counter counterTemplate = counterTemplates.get(entry.getKey());
      counter.counter.setColumn(counterTemplate.getColumnOffset());
      counter.counter.setColumnName(counterTemplate.getColumnName());
      counter.counter.setDataType(counterTemplate.getDataType());
      counter.counter.setTableName(counterTemplate.getTableName());
      if (counterTemplate.getLongCount() != null) {
        counter.counter.setDestTypeToLong();
      }
      else {
        counter.counter.setDestTypeToDouble();
      }

      Map<Object[], GroupCounter> map = getOrCreateInnerMap(counter);
      map.put(groupValues, counter);
    }
  }

  private Map<Object[], GroupCounter> getOrCreateInnerMap(GroupCounter counter) {
    String key = counter.counter.getTableName() + ":" + counter.counter.getColumnName();
    Map<Object[], GroupCounter> map = groupCounters.get(key);
    if (map == null) {
      map = new ConcurrentSkipListMap<>(new Comparator<Object[]>(){
        @Override
        public int compare(Object[] o1, Object[] o2) {
          for (int i = 0; i < fieldContexts.size(); i++) {
            int value = fieldContexts.get(i).getComparator().compare(o1[i], o2[i]);
            if (value == 0) {
              continue;
            }
            if (value < 0) {
              return -1;
            }
            if (value > 0) {
              return 1;
            }
          }
          return 0;
        }
      });
      groupCounters.put(key, map);
    } return map;
  }

  public void deserialize(DataInputStream in, DatabaseCommon common, String dbName) throws IOException {
    int fieldCount = in.readInt();
    fieldContexts = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      FieldContext context = new FieldContext();
      context.fieldName = in.readUTF();
      context.fieldOffset = in.readInt();
      context.dataType = DataType.Type.valueOf(in.readInt());
      context.tableSchema = new TableSchema();
      context.comparator = context.dataType.getComparator();
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(context.fieldName);
      fieldSchema.setType(context.dataType);
      context.tableSchema.addField(fieldSchema);
      fieldContexts.add(context);
    }
    groupCounters = new ConcurrentHashMap<>();
    int count = in.readInt();
    for (int i = 0; i < count; i++) {
      int innerCount = in.readInt();
      for (int j = 0; j < innerCount; j++) {
        GroupCounter groupCounter = new GroupCounter();
        Object[] groupValues = new Object[fieldContexts.size()];
        for (int k = 0; k < groupValues.length; k++) {
          int len = (int) DataUtil.readVLong(in);
          byte[] buffer = new byte[len];
          in.readFully(buffer);
          groupValues[k] = DatabaseCommon.deserializeFields(dbName, common, buffer, 0, fieldContexts.get(k).tableSchema, common.getSchemaVersion(), null, new AtomicInteger(), true)[0];
        }
        Counter counter = new Counter();
        counter.deserialize(in);
        groupCounter.setGroupValues(groupValues);
        groupCounter.setCounter(counter);
        Map<Object[], GroupCounter> map = getOrCreateInnerMap(groupCounter);
        map.put(groupValues, groupCounter);
      }
    }

    counterTemplates = new ConcurrentHashMap<>();
    int templateCount = in.readInt();
    for (int i = 0; i < templateCount; i++) {
      String key = in.readUTF();
      Counter counterTemplate = new Counter();
      counterTemplate.deserialize(in);
      counterTemplates.put(key, counterTemplate);
    }

  }

  public byte[] serialize(DatabaseCommon common) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    int count = fieldContexts.size();
    out.writeInt(count);
    for (int i = 0; i < count; i++) {
      FieldContext context = fieldContexts.get(i);
      out.writeUTF(context.fieldName);
      out.writeInt(context.fieldOffset);
      out.writeInt(context.dataType.getValue());
      context.tableSchema = new TableSchema();
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(context.fieldName);
      fieldSchema.setType(context.dataType);
      context.tableSchema.addField(fieldSchema);
    }

    out.writeInt(groupCounters.size());
    for (Map.Entry<String, Map<Object[], GroupCounter>> innerMap : groupCounters.entrySet()) {
      out.writeInt(innerMap.getValue().values().size());
      for (GroupCounter counter : innerMap.getValue().values()) {
        for  (int i = 0; i < count; i++) {
          DatabaseCommon.serializeFields(new Object[]{counter.groupValues[i]}, out, fieldContexts.get(i).tableSchema, common.getSchemaVersion(), true);
        }
        out.write(counter.counter.serialize());
      }
    }
    out.writeInt(counterTemplates.size());
    for (Map.Entry<String, Counter> counterTemplate : counterTemplates.entrySet()) {
      out.writeUTF(counterTemplate.getKey());
      out.write(counterTemplate.getValue().serialize());
    }

    out.close();
    return bytesOut.toByteArray();
  }

  public void addCounterTemplate(Counter counterTemplate) {
    counterTemplates.put(counterTemplate.getTableName() + ":" + counterTemplate.getColumnName(), counterTemplate);
  }

  public Map<String, Counter> getCounterTemplates() {
    return counterTemplates;
  }

  public static class GroupCounter {
    private Object[] groupValues;
    private Counter counter = new Counter();

    public void setGroupValues(Object[] groupValues) {
      this.groupValues = groupValues;
    }

    public Counter getCounter() {
      return counter;
    }

    public void setCounter(Counter counter) {
      this.counter = counter;
    }
  }

  public Map<String, Map<Object[], GroupCounter>> getGroupCounters() {
    return groupCounters;
  }

  public List<FieldContext> getFieldContexts() {
    return fieldContexts;
  }
}
