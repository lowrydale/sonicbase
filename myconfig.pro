-injars       db/target/sonicbase-core-unobfuscated-0.9.2.2.jar
-outjars      db/target/sonicbase-core-0.9.2.2.jar
-libraryjars  <java.home>/lib/rt.jar
-printmapping sonicbase-obfuscate.map

-dontshrink
-dontoptimize
-dontpreverify

-keep public class com.sonicbase.client.DatabaseClient
-keep public class com.sonicbase.jdbcdriver.ConnectionProxy
-keepclassmembers class com.sonicbase.jdbcdriver.ConnectionProxy {
     **;
     com.sonicbase.client.DatabaseClient getDatabaseClient();
}
-keepclassmembers class com.sonicbase.client.DatabaseClient {
     **;
}

-dontwarn org.**, com.**