@echo off
setlocal

REM Find Python executable
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    set "PYTHON=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
) else (
    set "PYTHON=python"
)

set "CMD=%~1"
if "%CMD%"=="" set "CMD=help"

if /I "%CMD%"=="build" (
    docker compose build
    goto :eof
)
if /I "%CMD%"=="up" (
    docker compose up -d --build
    goto :eof
)
if /I "%CMD%"=="down" (
    docker compose down
    goto :eof
)
if /I "%CMD%"=="restart" (
    docker compose restart
    goto :eof
)
if /I "%CMD%"=="status" (
    docker compose ps
    goto :eof
)
if /I "%CMD%"=="logs" (
    docker compose logs -f --tail=100
    goto :eof
)
if /I "%CMD%"=="clean" (
    docker compose down -v --remove-orphans
    goto :eof
)
if /I "%CMD%"=="test" (
    "%PYTHON%" -m pytest tests/ -v --durations=10
    goto :eof
)
if /I "%CMD%"=="lint" (
    "%PYTHON%" -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501
    goto :eof
)
if /I "%CMD%"=="format" (
    "%PYTHON%" -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501 --fix
    goto :eof
)
if /I "%CMD%"=="simulator" (
    "%PYTHON%" src/producers/river_simulator.py
    goto :eof
)
if /I "%CMD%"=="api" (
    "%PYTHON%" -m uvicorn src.serving.api:app --reload --host 0.0.0.0 --port 8000
    goto :eof
)
if /I "%CMD%"=="dashboard" (
    "%PYTHON%" -m streamlit run src/dashboard/app.py --server.port 8501
    goto :eof
)

echo ======================================================================
echo 🌊 Big Data River Quality Platform - Windows CLI
echo ======================================================================
echo Infrastructure:
echo   manage.bat build        Build Docker images
echo   manage.bat up           Start all services in background
echo   manage.bat down         Stop all services
echo   manage.bat restart      Restart all services
echo   manage.bat status       Display status of containers
echo   manage.bat logs         Tail logs of containers
echo   manage.bat clean        Remove containers and volumes
echo.
echo Development ^& Testing:
echo   manage.bat test         Run 51 Pytest tests
echo   manage.bat lint         Run Ruff linter
echo   manage.bat format       Fix linting issues
echo.
echo Workflows:
echo   manage.bat simulator    Run IoT Hydrological Simulator
echo   manage.bat api          Run FastAPI Serving locally
echo   manage.bat dashboard    Run Streamlit Dashboard locally
echo ======================================================================
