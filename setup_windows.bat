@echo off
rem Double-click, or run from cmd:  setup_windows.bat
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_windows.ps1" %*
pause
