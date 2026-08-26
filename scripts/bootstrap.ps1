param(
    [string]$Python = "python",
    [switch]$SkipNpm
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$VenvDir = Join-Path $RepoRoot ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

Push-Location $RepoRoot
try {
    if (-not (Test-Path $VenvPython)) {
        Write-Host "Creating Python virtual environment..."
        & $Python -m venv $VenvDir
    }

    Write-Host "Installing Python packages..."
    & $VenvPython -m pip install -U pip
    & $VenvPython -m pip install -e ".[dev]"

    $EditablePackages = @(
        "libs\axdata_core",
        "packages\axdata-source-tdx",
        "packages\axdata-source-tdx-ext",
        "packages\axdata-source-tencent",
        "packages\axdata-source-cninfo",
        "packages\axdata-sdk"
    )

    foreach ($PackagePath in $EditablePackages) {
        & $VenvPython -m pip install -e $PackagePath
    }

    # 采集器插件包(axdata-collector-*),含共享 runner 支撑包
    Get-ChildItem -Directory (Join-Path $RepoRoot "packages") |
        Where-Object { $_.Name -like "axdata-collector-*" } |
        ForEach-Object { & $VenvPython -m pip install -e $_.FullName }

    if (-not $SkipNpm) {
        if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
            throw "npm was not found. Install Node.js first, or rerun with -SkipNpm to skip Web dependencies."
        }
        Write-Host "Installing Web dependencies..."
        npm install
    }

    Write-Host ""
    Write-Host "AxData workspace is ready."
    Write-Host "Start API: .\.venv\Scripts\python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8666 --reload"
    Write-Host "Start Web: npm run dev:web"
}
finally {
    Pop-Location
}
