@echo off
SetLocal EnableDelayedExpansion
set count=0
for /F "delims=" %%a in ('wmic ComputerSystem get TotalPhysicalMemory') do (
  set Zone=%%a
  set /a count=!count! + 1
  if !count! GTR 1 goto Exit
)
:Exit
echo %Zone%
