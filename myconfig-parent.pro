-injars       db/target/sonicbase-core-unobfuscated-1.2.10.jar
-outjars      db/target/sonicbase-core-1.2.10.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-parent.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod

-keep public class com.sonicbase.bench.CustomFunctions
-keep public class com.sonicbase.bench.TestPerformance
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
-keep public class com.sonicbase.common.WindowsTerminal
-keep public class com.sonicbase.query.impl.ResultSetImpl
-keep public class com.sonicbase.query.ResultSet
-keep public class com.sonicbase.util.DateUtils
-keep public class com.sonicbase.common.SchemaOutOfSyncException
-keep public class com.sonicbase.server.LogManager$ByteCounterStream

-keepclassmembers  class com.sonicbase.bench.TestPerformance {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.bench.CustomFunctions {
    !private <methods>;
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

-keepclassmembers class com.sonicbase.common.WindowsTerminal {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.query.impl.ResultSetImpl {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.query.ResultSet {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.util.DateUtils {
    !private <methods>;
}
-keepclassmembers class com.sonicbase.common.SchemaOutOfSyncException {
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
-keep public class com.sonicbase.procedure.MyStoredProcedure1
-keep public class com.sonicbase.procedure.MyStoredProcedure2
-keep public class com.sonicbase.procedure.MyStoredProcedure3
-keep public class com.sonicbase.procedure.MyStoredProcedure4
-keep public class com.sonicbase.procedure.Parameters
-keep public class com.sonicbase.procedure.Record
-keep public class com.sonicbase.procedure.RecordEvaluator
-keep public class com.sonicbase.procedure.SonicBaseConnection
-keep public class com.sonicbase.procedure.SonicBasePreparedStatement
-keep public class com.sonicbase.procedure.StoredProcedure
-keep public class com.sonicbase.procedure.StoredProcedureContext
-keep public class com.sonicbase.procedure.StoredProcedureResponse
-keep public class com.sonicbase.procedure.StoredProcedureClient

-keepclassmembers  class com.sonicbase.procedure.StoredProcedureClient {
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
-keepclassmembers  class com.sonicbase.procedure.Parameters {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.Record {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.RecordEvaluator {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.SonicBaseConnection {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.SonicBasePreparedStatement {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.StoredProcedure {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.StoredProcedureContext {
    !private <methods>;
}
-keepclassmembers  class com.sonicbase.procedure.StoredProcedureResponse {
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