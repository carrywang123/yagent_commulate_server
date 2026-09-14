@echo off
REM 启动跨端中心服（Windows）
REM   run.bat                     正常启动
REM   run.bat --reload            开发模式，改代码自动重启
REM   run.bat --log-level DEBUG   看详细日志
REM   run.bat --no-log-file       只输出到控制台，不写文件
REM
REM 日志默认同时输出到控制台和 logs\relay.log（单文件 10MB，保留 5 个历史）。
REM
REM 【编码约定】本文件必须保存为 ANSI/GBK(cp936)，不要存成 UTF-8。
REM   cmd 是按“当前代码页”逐行解码 bat 的，进程起来时是 936；
REM   所以中文只能出现在 936 生效的区间，chcp 65001 之后的区间一律只写 ASCII，
REM   否则就会出现满屏乱码 + “不是内部或外部命令”，连 title 都会被打断。

title 跨端中心服 - Relay 8770

cd /d "%~dp0"

REM 下面切到 UTF-8，让 Python 打印的中文不乱码；从这里到 chcp 936 之间只允许 ASCII。
chcp 65001 >nul
set PYTHONPATH=src;%PYTHONPATH%
set PYTHONIOENCODING=utf-8

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -m relay %*
) else (
    python -m relay %*
)
REM keep the real exit code: the chcp below would reset errorlevel
set "EXITCODE=%errorlevel%"

REM switch back to cp936 so the Chinese lines below are decoded correctly
chcp 936 >nul

REM 异常退出时留住窗口，否则双击启动看不到报错就闪退了
if not "%EXITCODE%"=="0" (
    echo.
    echo [启动失败] 退出码 %EXITCODE%，上方为错误信息。
    echo 日志文件: %~dp0logs\relay.log
    pause
)
