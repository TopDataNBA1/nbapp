@echo off
REM ================================
REM Mantener workflows activos
REM ================================

cd /d C:\Users\USUARIO\Desktop\con Grok\web

REM Añadir cualquier cambio pendiente
git add -A

REM Traer cambios del remoto con rebase
git pull --rebase

REM Crear commit vacío
git commit --allow-empty -m "keep workflow alive"

REM Hacer push
git push

echo Workflow alive updated successfully!
pause
