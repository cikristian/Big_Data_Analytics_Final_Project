@echo off
rem Double-click, or run from cmd:  run_all.bat   /   run_all.bat -SkipDb   /   run_all.bat -From 4
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_all.ps1" %*
pause
