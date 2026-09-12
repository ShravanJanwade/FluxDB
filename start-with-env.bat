@echo off
REM Rustup configures the installed MSVC toolchain automatically.
call "%~dp0run-fluxdb.bat" %*
exit /b %errorlevel%
