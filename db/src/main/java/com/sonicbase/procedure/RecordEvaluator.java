/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

public interface RecordEvaluator {

  boolean evaluate(final StoredProcedureContext context, Record record);
}
