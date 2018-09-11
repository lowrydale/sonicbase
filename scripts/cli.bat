
SET _XMX_=1g
SET SONIC_BASE_HOME=%cd%\..
SET LOG4J_FILE=cli-log4j.xml
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%/logs/gc-cli.log

runclass.bat com.sonicbase.cli.Cli %*
