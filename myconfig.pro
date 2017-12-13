-injars       db/target/sonicbase-core-unobfuscated-1.2.8.jar
-outjars      db/target/sonicbase-core-1.2.8.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-obfuscate.map

-dontshrink
-dontoptimize


-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod

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

-keep public class com.sonicbase.queue.LocalMessageQueueProducer
-keep public class com.sonicbase.queue.AWSSQSMessageQueueProducer
-keep public class com.sonicbase.queue.KafkaMessageQueueProducer
-keep public class com.sonicbase.queue.MessageQueueProducer
-keep public class com.sonicbase.queue.LocalMessageQueueConsumer
-keep public class com.sonicbase.queue.AWSSQSMessageQueueConsumer
-keep public class com.sonicbase.queue.KafkaMessageQueueConsumer
-keep public class com.sonicbase.queue.MessageQueueConsumer
-keep public class com.sonicbase.queue.Message
-keep public class com.sonicbase.research.socket.NettyServer$MyChannelInitializer
-keep public class com.sonicbase.research.socket.NettyServer$ServerHandler
-keep public class com.sonicbase.server.MethodInvoker

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