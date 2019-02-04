SET SONIC_BASE_HOME=%cd%\..

SET _XMX_=512m

mkdir %SONIC_BASE_HOME%\logs

SET _test=%SONIC_BASE_HOME%
SET _result=%_test:~1,1%
if NOT "%_result%" == ":" (
     SET SONIC_BASE_HOME=%userprofile%\%SONIC_BASE_HOME%
)

pushd %SONIC_BASE_HOME%\bin
echo "start-SONIC_BASE_HOME=%SONIC_BASE_HOME%"

SET _LOG4J_FILENAME_=%SONIC_BASE_HOME%\logs\controller.log
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%\logs\gc-controller.log
SET LOG4J_FILE="log4j.xml"

%SONIC_BASE_HOME%\bin\runclass.bat com.sonicbase.controller.HttpServer > %SONIC_BASE_HOME%\logs\controller-sysout.log

popd