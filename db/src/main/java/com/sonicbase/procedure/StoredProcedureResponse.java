package com.sonicbase.procedure;

import java.util.List;

public interface StoredProcedureResponse {

  List<Record> getRecords();

  void setRecords(List<Record> records);

  void addRecord(Record record);
}
