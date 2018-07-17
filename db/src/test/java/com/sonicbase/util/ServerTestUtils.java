/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.util;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.server.DatabaseServer;
import org.jetbrains.annotations.NotNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerTestUtils {

  @NotNull
  static DatabaseCommon createCommon(DatabaseServer server) {
    DatabaseCommon common = mock(DatabaseCommon.class);
    when(server.getCommon()).thenReturn(common);
    when(common.getSchemaVersion()).thenReturn(10);
    return common;
  }
}
