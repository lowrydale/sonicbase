package com.sonicbase.procedure;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class StoredProcedureResponseImpl implements StoredProcedureResponse {
  private final DatabaseCommon common;
  private List<Record> records = new ArrayList<>();

  public StoredProcedureResponseImpl(DatabaseCommon common, ComObject comObject) {
    this.common = common;

    ComArray array = comObject.getArray(ComObject.Tag.RECORDS);
    if (array != null) {
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject recordObj = (ComObject) array.getArray().get(i);
        Record record = new RecordImpl(common, recordObj);
        records.add(record);
      }
    }
  }

  public StoredProcedureResponseImpl(DatabaseCommon common) {
    this.common = common;
  }

  @Override
  public List<Record> getRecords() {
    return records;
  }

  @Override
  public void setRecords(List<Record> records) {
    this.records = records;
  }

  @Override
  public void addRecord(Record record) {
    records.add(record);
  }

  public ComObject serialize() {
    ComObject ret = new ComObject();
    ComArray array = ret.putArray(ComObject.Tag.RECORDS, ComObject.Type.OBJECT_TYPE);
    for (Record record : records) {
      array.add(((RecordImpl)record).serialize());
    }
    return ret;
  }
}
