@echo off

wmic path Win32_PerfformattedData_PerfProc_Process where "IDProcess=%1" get IDProcess,PercentProcessorTime > temp-%1.txt
@setlocal enableextensions enabledelayedexpansion
@echo off
set /a "line = 0"
for /F "delims=" %%i in (' type "temp-%1.txt"') do set "thing=%%i"
echo %thing%