@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==================================================
echo   FreeCore-HeyBox (小黑盒认知桥接与控制网关)
echo ==================================================
echo 正在启动 Python 环境与服务 (未登录时将自动打印二维码)...
".venv\Scripts\python.exe" bridge.py
pause
