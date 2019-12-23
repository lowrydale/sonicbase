SET SONIC_BASE_HOME=%~dp0..

schtasks /end /tn "SonicBaseServer"
schtasks /delete /f /tn "SonicBaseServer"
schtasks /create /tn "SonicBaseServer" /tr "%SONIC_BASE_HOME%/bin/start-db-server.vbs %SONIC_BASE_HOME%/bin/start-db-server.bat %1 %2 %3 %4" /sc ONEVENT /EC Application
schtasks /run /tn "SonicBaseServer"
