
reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set OS=32BIT || set OS=64BIT


if "%_XMX_%" == "" (
  SET _XMX_=2000m
)
SET _GC_=%_GC_LOG_FILENAME_%

if "%_GC_LOG_FILENAME_%" == "" (
  SET _GC_=%SEARCH_HOME%/logs/gc.log
)

SET java_opts=-server  -XX:NewRatio=2 -XX:SurvivorRatio=10 -XX:+UseG1GC -XX:MaxGCPauseMillis=150 -XX:-ResizePLAB -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:-OmitStackTraceInFastThrow
SET java_opts=%java_opts% -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=2m -Xloggc:%_GC_%

if "%_LOG4J_FILENAME_%" == "" (
  SET _LOG4J_FILENAME_=out.log
)

if "%_ERROR_LOG4J_FILENAME_%" == "" (
  SET _ERROR_LOG4J_FILENAME_=errors.log
)

if "%_CLIENT_ERROR_LOG4J_FILENAME_%" == "" (
  SET _CLIENT_ERROR_LOG4J_FILENAME_=client-errors.log
)


SET java_opts=%java_opts% -Dlogfilename=%_LOG4J_FILENAME_% -DerrorLogfilename=%_ERROR_LOG4J_FILENAME_% -DclientErrorLogfilename=%_CLIENT_ERROR_LOG4J_FILENAME_%

SET java_opts=%java_opts% -XX:PermSize=256m -XX:MaxPermSize=356m -XX:+CMSClassUnloadingEnabled -XX:+PrintGCDetails  -XX:-UseLargePagesIndividualAllocation
SET java_opts=%java_opts% -XX:+HeapDumpOnOutOfMemoryError  -Djava.net.preferIPv4Stack=true
SET java_opts=%java_opts% -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
SET java_opts=%java_opts% -XX:-OmitStackTraceInFastThrow -XX:-UseLoopPredicate
SET java_opts=%java_opts% -Xmx%_XMX_% -XX:MaxDirectMemorySize=2g
SET java_opts=%java_opts% -Dfamilysearch.search.disable.foreign.node.registration -Dfamilysearch.search.disable.central.log
if "%OS%"=="64BIT" (
    SET java_opts=%java_opts% -Djava.library.path=%SEARCH_HOME%/lib/win/x64
)
if "%OS%"=="32BIT" (
    SET java_opts=%java_opts% -Djava.library.path=%SEARCH_HOME%/lib/win/x86
)

if "%LOG4J_FILE%" == "cli-log4j.xml" (
    SET java_opts=%java_opts% -Dlog4j.configuration=cli-log4j.xml
) ELSE (
    SET java_opts=%java_opts% -Dlog4j.configuration=log4j.xml
)

SET _JAVA_OPTS_=%java_opts%

setlocal EnableDelayedExpansion

SET _SEARCH_CLASSPATH_=%SEARCH_HOME%/target/;%SEARCH_HOME%/config/;%SEARCH_HOME%/lib
for /r ../lib %%i in (*.*) do (
    if NOT "%%i" == "win-util.dll" (
        SET _SEARCH_CLASSPATH_=!_SEARCH_CLASSPATH_!;%%~fi
    )
)
for /r lib %%i in (*.*) do (
    if NOT "%%i" == "win-util.dll" (
        SET _SEARCH_CLASSPATH_=!_SEARCH_CLASSPATH_!;%%~fi
    )
)

pushd ..
echo %SEARCH_HOME%

if "%OS%"=="64BIT" (
    echo %SEARCH_HOME%/bin/win/x64/ansicon.exe java %_JAVA_OPTS_% %_SYS_PROPS_% -classpath %_SEARCH_CLASSPATH_% %*
    ::%SEARCH_HOME%/bin/win/x64/ansicon.exe
    java %_JAVA_OPTS_% %_SYS_PROPS_% -classpath %_SEARCH_CLASSPATH_% %*
)
if "%OS%"=="32BIT" (
    ::%SEARCH_HOME%/bin/win/x86/ansicon.exe
    java %_JAVA_OPTS_% %_SYS_PROPS_% -classpath %_SEARCH_CLASSPATH_% %*
)

popd

