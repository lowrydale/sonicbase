-injars       jdbc/target/sonicbase-jdbc-unobfuscated-1.2.7.jar
-outjars      jdbc/target/sonicbase-jdbc-1.2.7.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-obfuscate-jdbc.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod


-keep public class Driver
-keep public class ReconfigureResults
-keep public class ConnectionProxy
-keep public enum com.sonicse.jdbcdriver.ConnectionProxy$Replica
-keep public class ResultSetProxy
-keep public class StatementProxy
-keep public class MemUtil
-keep public class ComObject
-keep public enum ComObject$Tag
-keep public class ComArray
-keep public class DataType
-keep public class FieldSchema
-keep public class TableSchema
-keep public enum DataType$Type
-keep public class DatabaseException
-keep public class LicenseOutOfComplianceException
-keep public class com.sonicbase.common.WindowsTerminal
-keep public class ResultSetImpl
-keep public class ResultSet
-keep public class DateUtils
-keep public class SchemaOutOfSyncException
-keep public class com.sonicbase.server.LogManager$ByteCounterStream

-keepclassmembers  class Driver {
    !private <methods>;
}
-keepclassmembers class ReconfigureResults {
    !private <methods>;
}
-keepclassmembers class ConnectionProxy {
    !private <methods>;
}
-keepclassmembers enum ConnectionProxy$Replica {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class ResultSetProxy {
    !private <methods>;
}
-keepclassmembers class StatementProxy {
    !private <methods>;
}
-keepclassmembers class MemUtil {
    !private <methods>;
}
-keepclassmembers class ComObject {
    !private <methods>;
}
-keepclassmembers enum ComObject$Tag {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class ComArray {
    !private <methods>;
}
-keepclassmembers class DataType {
    !private <methods>;
}
-keepclassmembers class com.sonicbse.schema.FieldSchema {
    !private <methods>;
}
-keepclassmembers class TableSchema {
    !private <methods>;
}
-keepclassmembers enum DataType$Type {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class DatabaseException {
    !private <methods>;
}
-keepclassmembers class LicenseOutOfComplianceException {
    !private <methods>;
}

-keepclassmembers class com.sonicbase.common.WindowsTerminal {
    !private <methods>;
}
-keepclassmembers class ResultSetImpl {
    !private <methods>;
}
-keepclassmembers class DateUtils {
    !private <methods>;
}
-keepclassmembers class SchemaOutOfSyncException {
    !private <methods>;
}

-dontwarn org.**, com.**