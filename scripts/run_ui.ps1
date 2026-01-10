# ---------------------------------------------
# Run Streamlit UI for FakeDataGenerator
# ---------------------------------------------

$ErrorActionPreference = "Stop"

$PROJECT_ROOT = Split-Path -Parent $PSScriptRoot
$VENV_PATH = Join-Path $PROJECT_ROOT ".venv"
$ACTIVATE_SCRIPT = Join-Path $VENV_PATH "Scripts\Activate.ps1"

Write-Host "Starting FakeDataGenerator UI..." -ForegroundColor Cyan

# Ensure virtual environment exists
if (-not (Test-Path $VENV_PATH)) {
    Write-Host "Virtual environment not found. Creating one..." -ForegroundColor Yellow
    python -m venv $VENV_PATH
}

# Activate virtual environment
if (-not (Test-Path $ACTIVATE_SCRIPT)) {
    throw "Virtual environment activation script not found."
}

& $ACTIVATE_SCRIPT

# Ensure dependencies are installed
if (-not (Get-Command streamlit -ErrorAction SilentlyContinue)) {
    Write-Host "Installing dependencies..." -ForegroundColor Yellow
    pip install -r (Join-Path $PROJECT_ROOT "requirements.txt")
}

# Run Streamlit
Write-Host "Launching Streamlit UI..." -ForegroundColor Green
Set-Location $PROJECT_ROOT
streamlit run ui/app.py
