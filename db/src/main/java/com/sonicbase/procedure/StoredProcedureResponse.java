package com.sonicbase.procedure;

import java.util.List;

public interface StoredProcedureResponse {

  /**
   * @return list of Records that will be returned to the client
   */
  List<Record> getRecords();

  /**
   * @param records list of records to set in the response
   */
  void setRecords(List<Record> records);

  /**
   * @param record a Record to add to the results
   */
  void addRecord(Record record);

}
