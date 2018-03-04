package com.sonicbase.procedure;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;

import java.util.ArrayList;
import java.util.List;

public class StoredProcedureResponseImpl implements StoredProcedureResponse {
  private List<Record> records = new ArrayList<>();

  public StoredProcedureResponseImpl(ComObject comObject) {
    ComArray array = comObject.getArray(ComObject.Tag.records);
    if (array != null) {
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject recordObj = (ComObject) array.getArray().get(i);
        Record record = new RecordImpl(recordObj);
        records.add(record);
      }
    }
  }

  public StoredProcedureResponseImpl() {

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
    ComArray array = ret.putArray(ComObject.Tag.records, ComObject.Type.objectType);
    for (Record record : records) {
      array.add(((RecordImpl)record).serialize());
    }
    return ret;
  }
}
