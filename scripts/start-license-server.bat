SET _XMX_=512m
SET SONIC_BASE_HOME=%2
mkdir %2\logs

SET _test=%2
SET _result=%_test:~1,1%
if NOT "%_result%" == ":" (
     SET SONIC_BASE_HOME=%userprofile%/%SONIC_BASE_HOME%
)

SET _LOG4J_FILENAME_=%SONIC_BASE_HOME%/logs/license-%1.log
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%/logs/gc-license-%1.log
SET LOG4J_FILE="log4j.xml"

pushd %SEARH_HOME%\bin

echo "starting LicenseServer: SONIC_BASE_HOME=" + %SONIC_BASE_HOME% + ", workingDir=" + %cd%

%SONIC_BASE_HOME%/bin/runclass.bat com.sonicbase.server.LicenseServer -port %1 > %SONIC_BASE_HOME%/logs/license-%1.sysout.log

popd