@echo off
REM 启动跨端文件中转服务（Blob Store）
REM   run_blob.bat                 正常启动（8771）
REM   run_blob.bat --reload        开发模式，改代码自动重启
REM   run_blob.bat --port 8899     换端口
REM   run_blob.bat --data-dir D:\blobs   换存储目录
REM
REM 与 Relay(8770) 刻意分成两个进程：
REM   Relay 是纯内存的转发管道，挂了重启即恢复；
REM   文件服务有磁盘状态，且大流量会与 Relay 的心跳争抢事件循环，
REM   一旦心跳超时 Relay 会断连，正在等待的跨端任务会被误判为对端掉线。
REM
REM 本文件必须保存为 ANSI/GBK(cp936)，不要存 UTF-8（同 run.bat）。

title 文件中转服 - Blob Store 8771

cd /d "%~dp0"

chcp 65001 >nul
set PYTHONPATH=src;%PYTHONPATH%
set PYTHONIOENCODING=utf-8

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -m blobstore %*
) else (
    python -m blobstore %*
)
set "EXITCODE=%errorlevel%"

chcp 936 >nul

if not "%EXITCODE%"=="0" (
    echo.
    echo [启动失败] 退出码 %EXITCODE%，上方为错误信息。
    echo 日志文件: %~dp0logs\blobstore.log
    pause
)
