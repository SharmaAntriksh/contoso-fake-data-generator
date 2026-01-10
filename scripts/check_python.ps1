$python = Get-Command python -ErrorAction SilentlyContinue

if (-not $python) {
    Write-Host "Python is not installed."
    Write-Host "Please install Python 3.10+ from https://www.python.org/downloads/"
    exit 1
}

$version = python --version
Write-Host "Found $version"
