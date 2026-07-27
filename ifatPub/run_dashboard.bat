@echo off
cd /d "%~dp0"

if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
)

call venv\Scripts\activate.bat

echo Installing / updating dependencies...
pip install -q streamlit plotly pandas gspread google-auth wordcloud python-bidi matplotlib

echo.
echo Starting dashboard at http://localhost:8501
streamlit run dashboard.py --server.port 8501 --server.address localhost
pause
