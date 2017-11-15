package com.sonicbase.common;

/**
 * Created by lowryda on 7/25/17.
 */
public @interface ExcludeRename {

  boolean exclude() default true;

  boolean applyToMembers() default true;
}
