SET _XMX_=%3
SET SONIC_BASE_HOME=%~dp0..

mkdir %SONIC_BASE_HOME/logs

pushd %SONIC_BASE_HOME%/bin
echo "start-SONIC_BASE_HOME=%SONIC_BASE_HOME%"

SET _CLIENT_ERROR_LOG4J_FILENAME_=%SONIC_BASE_HOME%/logs/client-errors.log
SET _ERROR_LOG4J_FILENAME_=%SONIC_BASE_HOME%/logs/errors.log
SET _LOG4J_FILENAME_=%SONIC_BASE_HOME%/logs/%2.log
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%/logs/gc-%2.log
SET LOG4J_FILE="log4j.xml"

%SONIC_BASE_HOME%/bin/runclass.bat com.sonicbase.server.NettyServer -host %1 -port %2 -gclog %_GC_LOG_FILENAME_% -xmx %_XMX_% -disable %4 -installDir %SONIC_BASE_HOME% > %SONIC_BASE_HOME%/logs/%2-sysout.log

popd