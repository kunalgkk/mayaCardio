@echo off
set PYTHONPATH=%~dp0;%PYTHONPATH%
python "%~dp0pipeline" %*
