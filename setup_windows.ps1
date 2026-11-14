<#
.SYNOPSIS
  One-time setup on Windows 10/11.

.DESCRIPTION
  1. Checks Python (3.10-3.12 recommended), Java (17 or 21) and Docker Desktop.
  2. Creates a virtual environment in .venv and installs requirements.txt.
  3. Downloads winutils.exe + hadoop.dll into tools\hadoop\bin.
     Spark needs these on Windows to write files (Parquet/CSV). spark\common.py
     finds them there automatically - no system settings to change.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
  (or double-click setup_windows.bat)
#>
param(
    [string]$Python = "python"   # e.g. -Python "py -3.11" if several versions are installed
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Say($msg)  { Write-Host "==> $msg" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "    OK   $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "    WARN $msg" -ForegroundColor Yellow }
function Fail($msg) { Write-Host "    FAIL $msg" -ForegroundColor Red; exit 1 }

function Native {
    # Run an external program and capture stdout+stderr as text. Windows
    # PowerShell 5.1 turns redirected stderr into errors, which would abort
    # the script under ErrorActionPreference=Stop (java -version writes to
    # stderr), so relax it just for this call.
    param([string]$Exe, [string[]]$ArgList)
    $old = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { $out = (& $Exe @ArgList 2>&1 | Out-String) } catch { $out = $null }
    $code = $LASTEXITCODE
    $ErrorActionPreference = $old
    return @{ Out = $out; Code = $code }
}

# Run "py -3.11" style commands as well as plain "python"
$pyParts = $Python -split " "
$pyExe = $pyParts[0]
$pyArgs = @()
if ($pyParts.Count -gt 1) { $pyArgs = $pyParts[1..($pyParts.Count - 1)] }

# ---------------------------------------------------------------- 1. Checks
Say "Checking prerequisites"

$r = Native $pyExe ($pyArgs + @("-c", "import sys; print('PYVER=%d.%d' % sys.version_info[:2])"))
$pyVer = $null
if ($r.Out -match 'PYVER=(\d+\.\d+)') { $pyVer = $Matches[1] }
if (-not $pyVer) {
    Fail "Python not found. Install Python 3.11 or 3.12 from https://www.python.org/downloads/ and tick 'Add python.exe to PATH'."
}
$pyMinor = [int]($pyVer.Split(".")[1])
if ($pyMinor -lt 10) { Fail "Python $pyVer is too old - install 3.11 or 3.12." }
if ($pyMinor -ge 13) { Warn "Python $pyVer detected. If 'pip install' fails on happybase/thriftpy2, install Python 3.12 and rerun with: -Python 'py -3.12'" }
else { Ok "Python $pyVer" }

$javaOut = (Native "java" @("-version")).Out
if (-not $javaOut -or $javaOut -notmatch 'version "(\d+)') {
    Fail "Java not found. Install Java 17 (https://adoptium.net/temurin/releases/?version=17), choose 'Set JAVA_HOME' in the installer, then open a NEW PowerShell window."
}
$javaMajor = [int]$Matches[1]
if ($javaMajor -lt 17) { Fail "Java $javaMajor found but Spark 4 needs Java 17 or 21. Install Temurin 17 and open a new terminal." }
Ok "Java $javaMajor"
if (-not $env:JAVA_HOME) { Warn "JAVA_HOME is not set. Spark usually still works if java is on PATH; if not, set JAVA_HOME to the JDK folder." }

$dockerOk = ((Native "docker" @("info")).Code -eq 0)
if ($dockerOk) { Ok "Docker Desktop is running" }
else { Warn "Docker is not running. Start Docker Desktop before using MongoDB/HBase (or use run_all.ps1 -SkipDb)." }

# ---------------------------------------------------------------- 2. venv + packages
Say "Creating virtual environment (.venv) and installing packages"
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    & $pyExe @pyArgs -m venv .venv
    if ($LASTEXITCODE -ne 0) { Fail "Could not create .venv" }
}
$venvPy = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
& $venvPy -m pip install --upgrade pip --quiet
& $venvPy -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { Fail "pip install failed (see the error above)." }
Ok "Packages installed into .venv"

# ---------------------------------------------------------------- 3. winutils
Say "Installing winutils.exe + hadoop.dll into tools\hadoop\bin"
$binDir = Join-Path $PSScriptRoot "tools\hadoop\bin"
New-Item -ItemType Directory -Force -Path $binDir | Out-Null

# Community builds of Hadoop's Windows binaries; the first source that works is used.
$sources = @(
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin",
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin",
    "https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.4.0-win10-x64/bin",
    "https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.1/bin"
)
foreach ($file in @("winutils.exe", "hadoop.dll")) {
    $target = Join-Path $binDir $file
    if ((Test-Path $target) -and ((Get-Item $target).Length -gt 10000)) { Ok "$file already present"; continue }
    $done = $false
    foreach ($src in $sources) {
        try {
            Invoke-WebRequest -Uri "$src/$file" -OutFile $target -UseBasicParsing -TimeoutSec 60
            if ((Get-Item $target).Length -gt 10000) { Ok "$file  <-  $src"; $done = $true; break }
        } catch { }
    }
    if (-not $done) {
        Remove-Item $target -ErrorAction SilentlyContinue
        Warn "Could not download $file automatically."
        Warn "Download it by hand from https://github.com/cdarlint/winutils (folder hadoop-3.3.6\bin)"
        Warn "and save it as $target"
    }
}

# ---------------------------------------------------------------- done
Write-Host ""
Say "Setup finished. Next:"
Write-Host "    docker compose up -d                 # start MongoDB + HBase (wait ~60 s)"
Write-Host "    .\run_all.ps1                        # full pipeline"
Write-Host "    .\run_all.ps1 -SkipDb                # Spark + charts only, no Docker needed"
