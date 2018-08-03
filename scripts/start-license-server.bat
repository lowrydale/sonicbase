SET _XMX_=512m
SET SEARCH_HOME=%2
mkdir %2\logs

SET _test=%2
SET _result=%_test:~1,1%
if NOT "%_result%" == ":" (
     SET SEARCH_HOME=%userprofile%/%SEARCH_HOME%
)

SET _LOG4J_FILENAME_=%SEARCH_HOME%/logs/license-%1.log
SET _GC_LOG_FILENAME_=%SEARCH_HOME%/logs/gc-license-%1.log
SET LOG4J_FILE="log4j.xml"

pushd %SEARH_HOME%\bin

echo "starting LicenseServer: search_home=" + %SEARCH_HOME% + ", workingDir=" + %cd%

%SEARCH_HOME%/bin/runclass.bat com.sonicbase.server.LicenseServer -port %1 > %SEARCH_HOME%/logs/license-%1.sysout.log

popd