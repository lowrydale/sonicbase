SET SONIC_BASE_HOME=%~dp0..

schtasks /end /tn "BenchServer"
schtasks /delete /f /tn "BenchServer"
schtasks /create /tn "BenchServer" /tr "%SONIC_BASE_HOME%/bin/start-bench-server.vbs %SONIC_BASE_HOME%/bin/start-bench-server.bat %1"  /sc ONEVENT /EC Application
schtasks /run /tn "BenchServer"
