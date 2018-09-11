SET SONIC_BASE_HOME=%2

schtasks /end /tn "BenchServer"
schtasks /delete /f /tn "BenchServer"
schtasks /create /tn "BenchServer" /tr "%SONIC_BASE_HOME%/bin/start-bench-server.vbs %SONIC_BASE_HOME%/bin/start-bench-server.bat %1 %2"  /sc ONEVENT /EC Application
schtasks /run /tn "BenchServer"
