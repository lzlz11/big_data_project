@echo off
echo ============================================================
echo   Weather x Restaurant - City Dining Score
echo   Big Data Project - ISEP
echo ============================================================
echo.

REM Check Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not running! Please start Docker Desktop first.
    pause
    exit /b 1
)

echo [1/4] Starting all services (Airflow, Elasticsearch, Kibana)...
docker-compose up -d
if %errorlevel% neq 0 (
    echo [ERROR] Failed to start services.
    pause
    exit /b 1
)

echo.
echo [2/4] Waiting for services to initialize (90 seconds)...
timeout /t 90 /nobreak > nul

echo.
echo [3/4] Setting up Kibana dashboard...
python kibana\setup_kibana.py

echo.
echo [4/4] All done!
echo.
echo ============================================================
echo   Airflow:       http://localhost:8080  (admin / admin)
echo   Kibana:        http://localhost:5601
echo   Elasticsearch: http://localhost:9200
echo ============================================================
echo.
echo Go to Airflow, enable the DAG and click the Play button!
echo.
pause
