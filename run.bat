@echo off
echo ============================================
echo   SME IPO Tracker - Starting...
echo ============================================

cd /d "%~dp0"

:: Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

:: Install dependencies if needed
echo Checking dependencies...
python -m pip install -r requirements.txt -q

:: Launch Streamlit
echo Launching dashboard in your browser...
python -m streamlit run app.py --server.port 8501

pause
