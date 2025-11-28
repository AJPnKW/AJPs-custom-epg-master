param(
    [switch]$Strict
)

$mode = $Strict.IsPresent ? "Strict" : "Soft"

Write-Host "=== TEST: Step 1 ==="
.\step1_extract_all_sites_master_channels.ps1 -Mode $mode

Write-Host "=== TEST: Step 2 ==="
.\step2_enrich_with_preferred_channels.ps1 -Mode $mode

Write-Host "=== TEST SUMMARY ==="
Get-ChildItem ..\baseline | Select-Object LastWriteTime, Name
