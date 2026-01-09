# ------------------------------------------------------------
# Create virtual environment (one-time setup)
# Always creates .venv at project root
# DOES NOT activate the environment
# ------------------------------------------------------------

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Resolve project root (parent of /scripts)
# ------------------------------------------------------------
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$VenvPath    = Join-Path $ProjectRoot ".venv"
$ReqFile     = Join-Path $ProjectRoot "requirements.txt"

# ------------------------------------------------------------
# Guard: already exists
# ------------------------------------------------------------
if (Test-Path $VenvPath) {
    Write-Host "Virtual environment already exists at $VenvPath" -ForegroundColor Yellow
    return
}

# ------------------------------------------------------------
# Guard: Python availability
# ------------------------------------------------------------
try {
    python --version | Out-Null
} catch {
    Write-Host "ERROR: Python not found. Install Python 3.9+ and add it to PATH." -ForegroundColor Red
    exit 1
}

# ------------------------------------------------------------
# Create venv
# ------------------------------------------------------------
Write-Host "Creating virtual environment at $VenvPath" -ForegroundColor Cyan
python -m venv $VenvPath

# ------------------------------------------------------------
# Install dependencies (without activating)
# ------------------------------------------------------------
if (Test-Path $ReqFile) {
    Write-Host "Installing dependencies from requirements.txt..." -ForegroundColor Yellow
    & (Join-Path $VenvPath "Scripts\python.exe") -m pip install -r $ReqFile
} else {
    Write-Host "requirements.txt not found. Skipping dependency install." -ForegroundColor Yellow
}

Write-Host "Virtual environment created successfully." -ForegroundColor Green
