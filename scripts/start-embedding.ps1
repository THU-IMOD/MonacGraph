# Creates embedding-service/.venv if needed and serves http://127.0.0.1:8099
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\env.ps1"

$Service = Join-Path $Root "embedding-service"
$Venv = Join-Path $Service ".venv"
$Port = if ($env:EMBEDDING_PORT) { $env:EMBEDDING_PORT } else { "8099" }

function Get-HostPython {
    foreach ($name in @("python", "python3")) {
        $command = Get-Command $name -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }
    }
    $py = Get-Command "py" -ErrorAction SilentlyContinue
    if ($py) {
        return "py"
    }
    throw "Python 3.10+ is required. Install Python and retry."
}

$VenvPython = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Host "Creating embedding virtualenv at $Venv"
    $hostPython = Get-HostPython
    if ($hostPython -eq "py") {
        & py -3 -m venv $Venv
    } else {
        & $hostPython -m venv $Venv
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create $Venv"
    }
}

& $VenvPython -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('fastapi') and importlib.util.find_spec('sentence_transformers') and importlib.util.find_spec('torch') else 1)"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing CPU PyTorch and embedding-service requirements into $Venv"
    $pipArgs = @("-m", "pip", "install", "--upgrade", "pip")
    & $VenvPython @pipArgs
    & $VenvPython -m pip install --cache-dir $env:PIP_CACHE_DIR torch --index-url https://download.pytorch.org/whl/cpu
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install CPU PyTorch"
    }
    & $VenvPython -m pip install --cache-dir $env:PIP_CACHE_DIR -r (Join-Path $Service "requirements.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install embedding-service requirements"
    }
}

Write-Host "Embedding caches: HF_HOME=$env:HF_HOME"
Write-Host "Starting embedding service on http://127.0.0.1:$Port (first run downloads the model)"
Set-Location $Service
& $VenvPython -m uvicorn app:app --host 127.0.0.1 --port $Port
