$ErrorActionPreference = 'Stop'

$rootDir = Split-Path -Path $PSScriptRoot -Parent
Set-Location $rootDir

Write-Host "[RUN] Starting node 3"
python -m src.node --id 3
