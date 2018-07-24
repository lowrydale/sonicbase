package com.sonicbase.common;

public @interface ExcludeRename {

  boolean exclude() default true;

  boolean applyToMembers() default true;
}
