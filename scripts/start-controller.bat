@echo off
SET SONIC_BASE_HOME=%~dp0..

schtasks /end /tn "SonicBaseController"
schtasks /delete /f /tn "SonicBaseController"
schtasks /create /tn "SonicBaseController" /tr "%SONIC_BASE_HOME%/bin/do-start-controller.vbs %SONIC_BASE_HOME%/bin/do-start-controller.bat" /sc ONEVENT /EC Application
schtasks /run /tn "SonicBaseController"

popd