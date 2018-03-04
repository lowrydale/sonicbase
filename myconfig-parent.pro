-injars       obfuscated/target/sonicbase-parent-unobfuscated-1.2.10.jar
-outjars      obfuscated/target/sonicbase-parent-1.2.10.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-parent.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod


-keep public class com.sonicbase.bench.CustomFunctions
-keep public class com.sonicbase.bench.TestPerformance
-keep public class com.sonicbase.server.DatabaseServer
-keep public class Driver
-keep public class ReconfigureResults
-keep public class ConnectionProxy
-keep public enum ConnectionProxy$Replica
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

-keepclassmembers  class com.sonicbase.bench.TestPerformance {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.bench.CustomFunctions {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.server.DatabaseServer {
    !private <methods>;
    !private <fields>;
    !public <methods>;
    !public <fields>;
    public byte[] invokeMethod(byte[], boolean, boolean);
}

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
-keepclassmembers class FieldSchema {
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
-keepclassmembers class ResultSet {
    !private <methods>;
}
-keepclassmembers class DateUtils {
    !private <methods>;
}
-keepclassmembers class SchemaOutOfSyncException {
    !private <methods>;
}

-keep public class com.sonicbase.bench.TestPerformance {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.research.socket.NettyServer {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.misc.FindIdInSnapshot {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.misc.RecordLoader {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.misc.RecordValidator {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.misc.TestTableScan {
      public static void main(java.lang.String[]);
}

-keep public class com.sonicbase.streams.LocalProducer
-keep public class com.sonicbase.streams.AWSSQSProducer
-keep public class com.sonicbase.streams.AWSKinesisProducer
-keep public class com.sonicbase.streams.KafkaProducer
-keep public class com.sonicbase.streams.StreamsProducer
-keep public class com.sonicbase.streams.LocalConsumer
-keep public class com.sonicbase.streams.AWSSQSConsumer
-keep public class com.sonicbase.streams.AWSKinesisConsumer
-keep public class com.sonicbase.streams.KafkaConsumer
-keep public class com.sonicbase.streams.StreamsConsumer
-keep public class com.sonicbase.streams.Message
-keep public class com.sonicbase.research.socket.NettyServer$MyChannelInitializer
-keep public class com.sonicbase.research.socket.NettyServer$ServerHandler
-keep public class com.sonicbase.server.MethodInvoker

-keepclassmembers  class com.sonicbase.streams.AWSSQSProducer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.AWSKinesisProducer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.KafkaProducer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.StreamsProducer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.AWSSQSConsumer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.AWSKinesisConsumer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.KafkaConsumer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.StreamsConsumer {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.streams.Message {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.server.LogManager$ByteCounterStream {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.research.socket.NettyServer {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.misc.FindIdInSnapshot {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.misc.RecordLoader {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.misc.RecordValidator {
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
-keepclassmembers class com.sonicbase.server.MethodInvoker {
    !private <methods>;
}

-dontwarn org.**, com.**