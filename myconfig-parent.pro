-injars       db/target/sonicbase-core-unobfuscated-1.2.13.jar
-outjars      db/target/sonicbase-core-1.2.13.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-parent.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod

-keep public class CustomFunctions
-keep public class TestPerformance
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
-keep public class com.sonicbase.common.Logger
-keep public class com.sonicbase.server.HttpServer
-keep public class com.sonicbase.server.MonitorHandler


-keepclassmembers  class com.sonicbase.server.MonitorHandler {
    !private <methods>;
}

-keepclassmembers  class com.sonicbase.server.HttpServer {
    !private <methods>;
}

-keepclassmembers  class com.sonicbase.common.Logger {
    !private <methods>;
}

-keepclassmembers  class TestPerformance {
    !private <methods>;
}
-keepclassmembers  class CustomFunctions {
    !private <methods>;
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

-keep public class TestPerformance {
      public static void main(java.lang.String[]);
}
-keep public class com.sonicbase.server.NettyServer {
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
-keep public class com.sonicbase.server.NettyServer$MyChannelInitializer
-keep public class com.sonicbase.server.NettyServer$ServerHandler
-keep public class com.sonicbase.server.MethodInvoker
-keep public class com.sonicbase.procedure.MyStoredProcedure1
-keep public class com.sonicbase.procedure.MyStoredProcedure2
-keep public class com.sonicbase.procedure.MyStoredProcedure3
-keep public class com.sonicbase.procedure.MyStoredProcedure4
-keep public class Parameters
-keep public class Record
-keep public class RecordEvaluator
-keep public class SonicBaseConnection
-keep public class SonicBasePreparedStatement
-keep public class StoredProcedure
-keep public class StoredProcedureContext
-keep public class StoredProcedureResponse
-keep public class StoredProcedureClient
-keep public class com.sonicbase.misc.BenchmarkClient
-keep public class com.sonicbase.website.Tutorial
-keep public class com.sonicbase.misc.TestMissing


-keepclassmembers  class com.sonicbase.streams.LocalProducer {
    !private <methods>;
    !public <methods>;
}

-keepclassmembers  class com.sonicbase.misc.TestMissing {
      public static void main(java.lang.String[]);
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.misc.BenchmarkClient {
      public static void main(java.lang.String[]);
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.website.Tutorial {
      public static void main(java.lang.String[]);
    !private <methods>;
}
-keepclassmembers  class StoredProcedureClient {
      public static void main(java.lang.String[]);
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.MyStoredProcedure1 {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.MyStoredProcedure2 {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.MyStoredProcedure3 {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.MyStoredProcedure4 {
    !private <methods>;
}
-keepclassmembers  class Parameters {
    !private <methods>;
}
-keepclassmembers  class Record {
    !private <methods>;
}
-keepclassmembers  class RecordEvaluator {
    !private <methods>;
}
-keepclassmembers  class SonicBaseConnection {
    !private <methods>;
}
-keepclassmembers  class SonicBasePreparedStatement {
    !private <methods>;
}
-keepclassmembers  class StoredProcedure {
    !private <methods>;
}
-keepclassmembers  class StoredProcedureContext {
    !private <methods>;
}
-keepclassmembers  class StoredProcedureResponse {
    !private <methods>;
}
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
-keepclassmembers class com.sonicbase.server.NettyServer {
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
-keepclassmembers class com.sonicbase.server.NettyServer$ServerHandler {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.server.NettyServer$MyChannelInitializer {
    !private <methods>;
    private <methods>;
}
-keepclassmembers class com.sonicbase.server.MethodInvoker {
    !private <methods>;
}

-dontwarn org.**, com.**