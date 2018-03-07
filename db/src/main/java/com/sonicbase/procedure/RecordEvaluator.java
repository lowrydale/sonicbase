/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

public interface RecordEvaluator {

  /**
   * @param context StoredProcedureContext
   * @param record Record to evaluate
   * @return true if the record should be included in the results
   */
  boolean evaluate(final StoredProcedureContext context, Record record);
}
