# ------------------------------------------------------------
# Activate virtual environment
# Always activates .venv from project root
# MUST be dot-sourced
# ------------------------------------------------------------

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Enforce dot-sourcing
# ------------------------------------------------------------
if ($MyInvocation.InvocationName -ne '.') {
    Write-Host "ERROR: This script must be dot-sourced to activate the virtual environment." -ForegroundColor Red
    Write-Host "Run it like this:" -ForegroundColor Yellow
    Write-Host "  . .\scripts\activate_venv.ps1" -ForegroundColor Cyan
    return
}

# ------------------------------------------------------------
# Resolve project root (parent of /scripts)
# ------------------------------------------------------------
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$VenvPath    = Join-Path $ProjectRoot ".venv"
$Activate    = Join-Path $VenvPath "Scripts\Activate.ps1"

# ------------------------------------------------------------
# Guard: venv exists
# ------------------------------------------------------------
if (-not (Test-Path $Activate)) {
    Write-Host "Virtual environment not found at $VenvPath" -ForegroundColor Red
    Write-Host "Run scripts/create_venv.ps1 first." -ForegroundColor Yellow
    return
}

# ------------------------------------------------------------
# Activate
# ------------------------------------------------------------
& $Activate
