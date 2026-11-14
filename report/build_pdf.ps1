# Builds TECHNICAL_REPORT.pdf from TECHNICAL_REPORT.md on Windows.
# Needs pandoc (https://pandoc.org/installing.html) and Google Chrome or Microsoft Edge.
#   powershell -ExecutionPolicy Bypass -File report\build_pdf.ps1
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if (-not (Get-Command pandoc -ErrorAction SilentlyContinue)) {
    Write-Host "pandoc not found - install it from https://pandoc.org/installing.html and open a new terminal." -ForegroundColor Red
    exit 1
}

$browsers = @(
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
    "$env:LocalAppData\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
    "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe"
)
$browser = $browsers | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1
if (-not $browser) { Write-Host "Chrome or Edge not found." -ForegroundColor Red; exit 1 }

pandoc TECHNICAL_REPORT.md -s --embed-resources --css report.css --resource-path=. -o TECHNICAL_REPORT.html
if ($LASTEXITCODE -ne 0) { exit 1 }

$html = (Resolve-Path TECHNICAL_REPORT.html).Path
$pdf = Join-Path $PSScriptRoot "TECHNICAL_REPORT.pdf"
& $browser --headless --disable-gpu --no-pdf-header-footer "--print-to-pdf=$pdf" "file:///$($html -replace '\\','/')" 2>$null | Out-Null
Start-Sleep -Seconds 2
Remove-Item TECHNICAL_REPORT.html
Write-Host "wrote report\TECHNICAL_REPORT.pdf"
