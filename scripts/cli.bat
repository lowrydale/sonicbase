
SET _XMX_=1g
SET SONIC_BASE_HOME=%~dp0..
SET LOG4J_FILE=cli-log4j.xml
SET _GC_LOG_FILENAME_=%SONIC_BASE_HOME%/logs/gc-cli.log

%SONIC_BASE_HOME%/bin/runclass.bat com.sonicbase.cli.Cli %*
