@echo off
REM ================================
REM Mantener workflows activos
REM ================================

REM Ir al directorio del repo
cd /d C:\Users\USUARIO\Desktop\con Grok\web

REM Traer cambios del remoto
git pull --rebase

REM Crear commit vacío
git commit --allow-empty -m "keep workflow alive"

REM Hacer push
git push

REM Mensaje final
echo Workflow alive updated successfully!
pause
