@echo off
REM Starts the hourly scheduler with the project's virtualenv. Keep this window open;
REM close it or press Ctrl+C to stop. See README section 4.
cd /d "%~dp0"
".venv\Scripts\python.exe" -m src.scheduler %*
exit /b %ERRORLEVEL%
