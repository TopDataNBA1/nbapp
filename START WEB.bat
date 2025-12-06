@echo off
chcp 65001 >nul
title TOP DATA NBA LIVE - LOCAL SERVER
cls
echo.
echo  ██████╗ ██████╗  █████╗  ██████╗██╗  ██╗
echo  ██╔══██╗██╔══██╗██╔══██╗██╔════╝██║ ██╔╝
echo  ██║  ██║██████╔╝███████║██║     █████╔╝ 
echo  ██║  ██║██╔═══╝ ██╔══██║██║     ██╔═██╗ 
echo  ██████╔╝██║     ██║  ██║╚██████╗██║  ██╗
echo  ╚═════╝ ╚═╝     ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝
echo.
echo   SERVIDOR LOCAL INICIADO EN http://localhost:8000
echo   EL NAVEGADOR SE ABRIRÁ EN 3 SEGUNDOS...
echo   CIERRA ESTA VENTANA PARA APAGAR EL SERVIDOR
echo.
timeout /t 3 >nul
start http://localhost:8000
python -m http.server 8000