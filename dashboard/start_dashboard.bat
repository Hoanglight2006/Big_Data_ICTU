@echo off
REM =============================================================================
REM dashboard/start_dashboard.bat — Khoi chay Dashboard tren Windows
REM =============================================================================
cd /d "%~dp0\.."
set PORT=8050
if not "%~1"=="" set PORT=%~1

echo ==========================================================
echo  [INFO] KHOI CHAY BIG DATA LOG ANALYTICS DASHBOARD
echo  Dia chi truy cap: http://localhost:%PORT%
echo ==========================================================

python dashboard\server.py %PORT%
pause
