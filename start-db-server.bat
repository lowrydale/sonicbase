SET _XMX_=%3
SET SEARCH_HOME=%4
mkdir %4/logs


SET _test=%4
SET _result=%_test:~1,1%
if NOT "%_result%" == ":" (
     SET SEARCH_HOME=%userprofile%/%SEARCH_HOME%
)


cd %SEARCH_HOME%/bin
echo "start-search_home=%SEARCH_HOME%"

SET _CLIENT_ERROR_LOG4J_FILENAME_=%SEARCH_HOME%/logs/client-errors.log
SET _ERROR_LOG4J_FILENAME_=%SEARCH_HOME%/logs/errors.log
SET _LOG4J_FILENAME_=%SEARCH_HOME%/logs/%2.log
SET _GC_LOG_FILENAME_=%SEARCH_HOME%/logs/gc-%2.log
SET LOG4J_FILE="log4j.xml"

start /b "server:%2" %SEARCH_HOME%/bin/runclass.bat com.sonicbase.research.socket.NettyServer -host %1 -port %2 -cluster %5 -gclog %_GC_LOG_FILENAME_% -xmx %_XMX_% > %SEARCH_HOME%/logs/%2-sysout.log
