/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SchemaReadLock {
}
