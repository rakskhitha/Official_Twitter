@echo off
REM Entry point for Windows Task Scheduler. Runs the daily job with the project's virtualenv.
cd /d "%~dp0"
".venv\Scripts\python.exe" -m src.run_daily %*
exit /b %ERRORLEVEL%
