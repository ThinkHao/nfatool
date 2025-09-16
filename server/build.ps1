# Requires: Python 3.10+ and PyInstaller
param(
  [string]$Name = "nfa95"
)

$ErrorActionPreference = "Stop"

# Resolve directories
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerDir = Resolve-Path $ScriptDir
$RootDir = Resolve-Path (Join-Path $ServerDir "..")

# Ensure PyInstaller
try {
  pyinstaller --version | Out-Null
} catch {
  Write-Host "PyInstaller not found. Installing..." -ForegroundColor Yellow
  pip install pyinstaller
}

# Compose add-data arguments (Windows uses src;dest)
$StaticDir = Join-Path $ServerDir "static"
$MappingFile = Join-Path $ServerDir "mapping.json"
$AddData = @()
if (Test-Path $StaticDir) { $AddData += "--add-data=`"$StaticDir;static`"" }
if (Test-Path $MappingFile) { $AddData += "--add-data=`"$MappingFile;mapping.json`"" }

# Entry
$Entry = Join-Path $ServerDir "serve.py"
if (-not (Test-Path $Entry)) {
  Write-Error "Entry not found: $Entry"
  exit 1
}

# Clean previous build artifacts
Remove-Item -Recurse -Force (Join-Path $ServerDir "build") -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force (Join-Path $ServerDir "__pycache__") -ErrorAction SilentlyContinue

# Build
$Args = @(
  "--name", $Name,
  "--onefile",
  "--clean",
  "--noconfirm",
  "--paths", $RootDir,  # ensure 'server' package import works
  $AddData,
  $Entry
) | Where-Object { $_ -ne $null -and $_ -ne "" }

Write-Host "Running: pyinstaller $($Args -join ' ')" -ForegroundColor Cyan
pyinstaller @Args

$Exe = Join-Path (Join-Path $RootDir "dist") ("{0}.exe" -f $Name)
# Copy env example into dist for distribution
$EnvExample = Join-Path $ServerDir ".env.example"
if (Test-Path $EnvExample) {
  Copy-Item $EnvExample (Join-Path (Join-Path $RootDir "dist") ".env.example") -Force
}
if (Test-Path $Exe) {
  Write-Host "Build succeeded: $Exe" -ForegroundColor Green
  Write-Host "Deploy by copying the exe next to a .env file. Logs and storage will auto-create alongside the exe." -ForegroundColor Green
} else {
  Write-Warning "Build seems to have failed. Check the output above."
}
