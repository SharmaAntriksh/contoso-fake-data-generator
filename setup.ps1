# ------------------------------------------------------------
# Setup script for Windows (PowerShell)
# ------------------------------------------------------------

$ErrorActionPreference = "Stop"

Write-Host "Starting project setup..." -ForegroundColor Cyan

# ------------------------------------------------------------
# Check Python
# ------------------------------------------------------------
try {
    python --version | Out-Null
} catch {
    Write-Host "ERROR: Python not found. Install Python 3.9+ and add it to PATH." -ForegroundColor Red
    exit 1
}

# ------------------------------------------------------------
# Create virtual environment if missing
# ------------------------------------------------------------
if (-Not (Test-Path ".venv")) {
    Write-Host "Creating virtual environment..." -ForegroundColor Yellow
    python -m venv .venv
} else {
    Write-Host "Virtual environment already exists." -ForegroundColor Green
}

# ------------------------------------------------------------
# Activate virtual environment (PERSISTS)
# ------------------------------------------------------------
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
. .\.venv\Scripts\Activate.ps1

# ------------------------------------------------------------
# Upgrade pip
# ------------------------------------------------------------
Write-Host "Upgrading pip..." -ForegroundColor Yellow
python -m pip install --upgrade pip

# ------------------------------------------------------------
# Install dependencies
# ------------------------------------------------------------
if (-Not (Test-Path "requirements.txt")) {
    Write-Host "ERROR: requirements.txt not found in project root." -ForegroundColor Red
    exit 1
}

Write-Host "Installing dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
Write-Host ""
Write-Host "Setup complete." -ForegroundColor Green
Write-Host "Virtual environment is ACTIVE in this PowerShell session." -ForegroundColor Green
Write-Host ""
Write-Host "You can now run the project." -ForegroundColor Cyan
