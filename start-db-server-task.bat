SET SEARCH_HOME=%4

schtasks /end /tn "SonicBaseServer%1"
schtasks /delete /f /tn "SonicBaseServer%1"
schtasks /create /tn "SonicBaseServer%1" /tr "%SEARCH_HOME%/bin/start-db-server.vbs %SEARCH_HOME%/bin/start-db-server.bat %1 %2 %3 %4 %5" /sc once /sd 01/01/2003 /st 00:00
schtasks /run /tn "SonicBaseServer%1"
