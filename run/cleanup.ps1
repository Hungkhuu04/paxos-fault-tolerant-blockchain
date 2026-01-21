$ErrorActionPreference = 'Stop'
$rootDir = Split-Path -Path $PSScriptRoot -Parent
$dataDir = Join-Path $rootDir 'data'

Write-Host "[CLEANUP] Resetting blockchain.json, balances.json, and ledger_log.json for all nodes..."
Write-Host "Root directory: $rootDir"
Write-Host ""


foreach ($i in 1..5) {
    $nodeDir = Join-Path $dataDir ("P{0}" -f $i)
    $bcFile  = Join-Path $nodeDir 'blockchain.json'
    $balFile = Join-Path $nodeDir 'balances.json'
    $logFile = Join-Path $nodeDir 'ledger_log.json'

    if (Test-Path $nodeDir) {
        Write-Host "[CLEANUP] Processing $nodeDir"
        Remove-Item $bcFile, $balFile, $logFile -ErrorAction SilentlyContinue
        '[]' | Set-Content $bcFile -Encoding UTF8
        '{"P1":100,"P2":100,"P3":100,"P4":100,"P5":100}' | Set-Content $balFile -Encoding UTF8
        '{}' | Set-Content $logFile -Encoding UTF8

        Write-Host "  - blockchain.json reset"
        Write-Host "  - balances.json reset to default balances"
        Write-Host "  - ledger_log.json cleared"
        Write-Host ""
    }
    else {
        Write-Host "[CLEANUP] Skipping $nodeDir (does not exist)"
    }
}

Write-Host "[CLEANUP] Done!"
