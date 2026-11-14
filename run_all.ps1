<#
.SYNOPSIS
  End-to-end pipeline on Windows (PowerShell version of run_all.sh).

.EXAMPLE
  .\run_all.ps1                 # full run (needs `docker compose up -d` first)
  .\run_all.ps1 -SkipDb         # Spark + charts only, no MongoDB/HBase needed
  .\run_all.ps1 -Scale 0.25     # bigger dataset (default 0.1 = 200k sessions)
  .\run_all.ps1 -From 4         # resume from step 4 (Spark cleaning)

  If scripts are blocked:  powershell -ExecutionPolicy Bypass -File .\run_all.ps1
  (or double-click run_all.bat)
#>
param(
    [switch]$SkipDb,
    [double]$Scale = 0.1,
    [int]$From = 1
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

# Use the project's virtual environment when it exists (created by setup_windows.ps1)
$Py = "python"
foreach ($candidate in @(".venv/Scripts/python.exe", ".venv/bin/python")) {
    $full = Join-Path $PSScriptRoot $candidate
    if (Test-Path $full) { $Py = $full; break }
}

# Python workers started by Spark must use the same interpreter
$env:PYSPARK_PYTHON = $Py
$env:PYSPARK_DRIVER_PYTHON = $Py
$env:PYTHONIOENCODING = "utf-8"

function Step($n, $msg) { Write-Host ""; Write-Host "==> $n/7 $msg" -ForegroundColor Cyan }

function Run {
    # Run a Python script, stop the pipeline if it fails
    param([string]$Dir, [string[]]$ScriptArgs)
    Push-Location (Join-Path $PSScriptRoot $Dir)
    try {
        & $Py @ScriptArgs
        if ($LASTEXITCODE -ne 0) { throw "FAILED: python $($ScriptArgs -join ' ')  (exit code $LASTEXITCODE)" }
    } finally { Pop-Location }
}

$sw = [Diagnostics.Stopwatch]::StartNew()

if ($From -le 1) {
    if (-not (Test-Path "data/raw/transactions.json")) {
        Step 1 "Generating dataset (scale $Scale)"
        Run "." @("data_gen/dataset_generator.py", "--scale", "$Scale", "--out", "data/raw")
    } else {
        Step 1 "Dataset already in data\raw (delete that folder to regenerate)"
    }
}

if (-not $SkipDb) {
    if ($From -le 2) {
        Step 2 "Loading MongoDB + running aggregation pipelines"
        Run "." @("mongodb/load_mongodb.py")
        Run "." @("mongodb/aggregations.py")
    }
    if ($From -le 3) {
        Step 3 "Loading HBase + sample queries"
        Run "." @("hbase/load_hbase.py")
        Run "." @("hbase/query_hbase.py", "--user", "user_000042", "--product", "prod_00123")
    }
} elseif ($From -le 3) {
    Step "2-3" "Skipping MongoDB/HBase (-SkipDb)"
}

if ($From -le 4) {
    Step 4 "Spark: clean + normalize"
    Run "spark" @("01_clean_normalize.py")
}

if ($From -le 5) {
    Step 5 "Spark: cohort analysis, recommendations, Spark SQL"
    Run "spark" @("02_cohort_analysis.py")
    Run "spark" @("03_recommendations.py")
    Run "spark" @("04_spark_sql_analytics.py")
}

if ($From -le 6) {
    Step 6 "Integrated funnel + CLV analysis"
    if ($SkipDb) { $source = "files" } else { $source = "databases" }
    Run "." @("integration/funnel_clv_analysis.py", "--source", $source)
}

if ($From -le 7) {
    Step 7 "Charts"
    Run "." @("visualization/architecture_diagram.py")
    Run "." @("visualization/make_charts.py")
}

Write-Host ""
Write-Host ("==> Done in {0:N1} min - results in output\, charts in output\charts\" -f $sw.Elapsed.TotalMinutes) -ForegroundColor Green
