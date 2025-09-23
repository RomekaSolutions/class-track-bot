@echo off
setlocal
cd /d "%~dp0"
call venv\Scripts\activate.bat
"
set "ADMIN_IDS=1566976731"
set DEBUG_MODE=1
python class_track_bot.py
pause
