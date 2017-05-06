@echo off
SetLocal EnableDelayedExpansion
set count=0
for /F "delims=" %%a in ('wmic process where "name='java.exe' and CommandLine like '%%port %1%%'" get processid') do (
  set pid=%%a
  set /a count=!count! + 1
  if !count! GTR 1 goto Exit
)
:Exit

echo %pid%

if NOT "%pid" == "" (
    powershell -Command "Stop-Process -Id %pid%"

    SET val=not

    :while1
    if NOT "%val%" == "" (
        timeout 1
        set count=0
        for /F "delims=" %%a in ('wmic process where "name='java.exe' and CommandLine like '%%port %1%%'" get processid') do (
          set pid=%%a
          set /a count=!count! + 1
          if !count! GTR 1 goto Exit
        )
        :Exit
        if NOT "%pid" == "" (
            powershell -Command "Stop-Process -Id %pid%"

            goto :while1
        )
    )
)