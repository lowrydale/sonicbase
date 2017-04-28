SET _XMX_=4g
SET SEARCH_HOME=%4
mkdir %4\logs

SET _test=%4
SET _result=%_test:~1,1%
if NOT "%_result%" == ":" (
     SET SEARCH_HOME=%userprofile%/%SEARCH_HOME%
)

SET _LOG4J_FILENAME_=%SEARCH_HOME%/logs/bench-%2.log
SET _GC_LOG_FILENAME_=%SEARCH_HOME%/logs/gc-bench-%2.log
SET LOG4J_FILE="log4j.xml"
pushd bin
echo "starting BenchmarkServer: search_home=" + %SEARCH_HOME% + ", workingDir=" + %cd%
start /b "bench:%2" %SEARCH_HOME%/bin/runclass.bat com.sonicbase.bench.BenchServer -port %2 > %SEARCH_HOME%/logs/bench-%2.sysout.log
popd