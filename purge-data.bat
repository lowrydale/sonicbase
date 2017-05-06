@echo off
rd /s /q %1.last
rename %1 %1.last
rd /s /q %1
rd /s /q %1.last