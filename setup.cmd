@echo off
REM ==========================================
REM Setup PowerShell com Git e GitHub CLI
REM ==========================================

title SEFIN Audit - Git Environment
echo.
echo Iniciando PowerShell com ferramentas portáteis...
echo.

cd /d "%~dp0"

powershell -NoExit -Command ". %~dp0setup-env.ps1"
