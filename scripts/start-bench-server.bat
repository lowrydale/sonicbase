SET _XMX_=512m
SET SONIC_BASE_HOME=%~dp0..
mkdir %SONIC_BASE_HOME%\logs

SET _LOG4J_FILENAME_=%SONIC_BASE_HOME%/logs/bench-%1.log
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%/logs/gc-bench-%1.log
SET LOG4J_FILE="log4j.xml"

pushd %SONIC_BASE_HOME%\bin

echo "starting BenchmarkServer: SONIC_BASE_HOME=" + %SONIC_BASE_HOME% + ", workingDir=" + %cd%

%SONIC_BASE_HOME%/bin/runclass.bat com.sonicbase.bench.BenchServer -port %1 > %SONIC_BASE_HOME%/logs/bench-%1.sysout.log

popd