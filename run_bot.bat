@echo off
setlocal
cd /d "%~dp0"
call venv\Scripts\activate.bat
set "TELEGRAM_BOT_TOKEN=8199217407:AAHO3wURQ7Rdo6HshC1BZ2Mv_vvv-fkpEcU"
set "ADMIN_IDS=1566976731"
set DEBUG_MODE=1
python class_track_bot.py
pause
