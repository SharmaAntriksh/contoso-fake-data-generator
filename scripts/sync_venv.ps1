$VenvPath = ".venv"
$RequirementsFile = "requirements.txt"
$HashFile = "$VenvPath\.requirements.hash"

if (-not (Test-Path $VenvPath)) {
    Write-Host ".venv not found. Creating virtual environment..."
    python -m venv $VenvPath
}

# Activate venv
& "$VenvPath\Scripts\Activate.ps1"

if (-not (Test-Path $RequirementsFile)) {
    Write-Host "requirements.txt not found. Skipping dependency sync."
    return
}

# Compute current hash
$CurrentHash = (Get-FileHash $RequirementsFile -Algorithm SHA256).Hash

# Read previous hash (if any)
$PreviousHash = if (Test-Path $HashFile) {
    Get-Content $HashFile
} else {
    ""
}

if ($CurrentHash -ne $PreviousHash) {
    Write-Host "requirements.txt changed. Updating virtual environment..."
    pip install --upgrade pip
    pip install -r $RequirementsFile

    $CurrentHash | Out-File $HashFile -Encoding ascii
    Write-Host "Virtual environment updated."
}
else {
    Write-Host "Virtual environment already up to date."
}
