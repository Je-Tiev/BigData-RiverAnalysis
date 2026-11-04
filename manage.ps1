<#
.SYNOPSIS
    River Big Data & MLOps Platform - Windows Management CLI
.DESCRIPTION
    Equivalent to Makefile for Windows PowerShell environments.
.EXAMPLE
    .\manage.ps1 up
    .\manage.ps1 test
    .\manage.ps1 simulator
    .\manage.ps1 dashboard
#>

param (
    [Parameter(Position=0)]
    [string]$Command = "help"
)

$PythonExe = "python"
if (Get-Command "C:\Users\coc14\AppData\Local\Programs\Python\Python312\python.exe" -ErrorAction SilentlyContinue) {
    $PythonExe = "C:\Users\coc14\AppData\Local\Programs\Python\Python312\python.exe"
}

switch ($Command.ToLower()) {
    "build" {
        docker compose build
    }
    "up" {
        docker compose up -d
    }
    "down" {
        docker compose down
    }
    "restart" {
        docker compose restart
    }
    "status" {
        docker compose ps
    }
    "logs" {
        docker compose logs -f --tail=100
    }
    "clean" {
        docker compose down -v --remove-orphans
    }
    "test" {
        & $PythonExe -m pytest tests/ -v --durations=10
    }
    "lint" {
        & $PythonExe -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501
    }
    "format" {
        & $PythonExe -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501 --fix
    }
    "simulator" {
        & $PythonExe src/producers/river_simulator.py
    }
    "api" {
        & $PythonExe -m uvicorn src.serving.api:app --reload --host 0.0.0.0 --port 8000
    }
    "dashboard" {
        & $PythonExe -m streamlit run src/dashboard/app.py --server.port 8501
    }
    default {
        Write-Host "======================================================================" -ForegroundColor Cyan
        Write-Host "🌊 Big Data River Quality & MLOps Platform - PowerShell CLI" -ForegroundColor Green
        Write-Host "======================================================================" -ForegroundColor Cyan
        Write-Host "Infrastructure & Services:"
        Write-Host "  .\manage.ps1 build        Build Docker container images"
        Write-Host "  .\manage.ps1 up           Start full Big Data & AI platform in background"
        Write-Host "  .\manage.ps1 down         Stop all running services"
        Write-Host "  .\manage.ps1 restart      Restart the entire platform"
        Write-Host "  .\manage.ps1 status       Display container statuses and ports"
        Write-Host "  .\manage.ps1 logs         Tail logs of all platform containers"
        Write-Host "  .\manage.ps1 clean        Stop and remove all containers and volumes"
        Write-Host ""
        Write-Host "Development & Quality:"
        Write-Host "  .\manage.ps1 test         Execute full Pytest suite"
        Write-Host "  .\manage.ps1 lint         Run Ruff style and syntax checks"
        Write-Host "  .\manage.ps1 format       Auto-fix linting issues"
        Write-Host ""
        Write-Host "Workflows & Execution:"
        Write-Host "  .\manage.ps1 simulator    Run IoT Hydrological Simulator (Kafka Producer)"
        Write-Host "  .\manage.ps1 api          Run FastAPI Model Serving microservice locally"
        Write-Host "  .\manage.ps1 dashboard    Run Streamlit Operations Command Center locally"
        Write-Host "======================================================================" -ForegroundColor Cyan
    }
}
