-injars       db/target/sonicbase-core-unobfuscated-1.2.3.jar
-outjars      db/target/sonicbase-core-1.2.3.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-obfuscate.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod

-keep public class com.sonicbase.research.socket.NettyServer {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.research.socket.NettyServer$MyChannelInitializer
-keep public class com.sonicbase.research.socket.NettyServer$ServerHandler
-keep public class com.sonicbase.jdbcdriver.Driver
-keep public class com.sonicbase.client.ReconfigureResults
-keep public class com.sonicbase.jdbcdriver.ConnectionProxy
-keep public enum com.sonicbase.jdbcdriver.ConnectionProxy$Replica
-keep public class com.sonicbase.jdbcdriver.ResultSetProxy
-keep public class com.sonicbase.jdbcdriver.StatementProxy
-keep public class com.sonicbase.common.MemUtil
-keep public class com.sonicbase.common.ComObject
-keep public enum com.sonicbase.common.ComObject$Tag
-keep public class com.sonicbase.common.ComArray
-keep public class com.sonicbase.schema.DataType
-keep public class com.sonicbase.schema.FieldSchema
-keep public class com.sonicbase.schema.TableSchema
-keep public enum com.sonicbase.schema.DataType$Type
-keep public class com.sonicbase.query.DatabaseException
-keep public class com.sonicbase.common.LicenseOutOfComplianceException
-keep public class com.sonicbase.server.CommandHandler
-keep public class com.sonicbase.common.WindowsTerminal
-keep public class com.sonicbase.query.impl.ResultSetImpl
-keep public class com.sonicbase.query.ResultSet
-keep public class com.sonicbase.util.ISO8601
-keep public class com.sonicbase.util.JsonDict
-keep public class com.sonicbase.util.JsonArray
-keep public class com.sonicbase.util.StreamUtils
-keep public class com.sonicbase.common.SchemaOutOfSyncException
-keep public class com.sonicbase.server.LogManager$ByteCounterStream

-keepclassmembers  class com.sonicbase.server.LogManager$ByteCounterStream {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.research.socket.NettyServer {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.research.socket.NettyServer$ServerHandler {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.research.socket.NettyServer$MyChannelInitializer {
    !private <methods>;
    private <methods>;
}
-keepclassmembers  class com.sonicbase.jdbcdriver.Driver {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.client.ReconfigureResults {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.jdbcdriver.ConnectionProxy {
    !private <methods>;
}
-keepclassmembers enum com.sonicbase.jdbcdriver.ConnectionProxy$Replica {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class com.sonicbase.jdbcdriver.ResultSetProxy {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.jdbcdriver.StatementProxy {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.MemUtil {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.ComObject {
    !private <methods>;
}
-keepclassmembers enum com.sonicbase.common.ComObject$Tag {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class com.sonicbase.common.ComArray {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.schema.DataType {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.schema.FieldSchema {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.schema.TableSchema {
    !private <methods>;
}
-keepclassmembers enum com.sonicbase.schema.DataType$Type {
    !private <methods>;
    !private <fields>;
    public static **[] values(); public static ** valueOf(java.lang.String);
}
-keepclassmembers class com.sonicbase.query.DatabaseException {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.LicenseOutOfComplianceException {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.server.CommandHandler {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.WindowsTerminal {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.query.impl.ResultSetImpl {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.util.ISO8601 {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.util.JsonDict {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.util.JsonArray {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.util.StreamUtils {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.SchemaOutOfSyncException {
    !private <methods>;
}

-dontwarn org.**, com.**