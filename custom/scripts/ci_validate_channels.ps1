<#
ci_validate_channels.ps1
Fails CI if any input channel CSV has blank name/site/xmltv_id.
Local-only file checks.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$BasePath = Resolve-Path (Join-Path $PSScriptRoot "..\..") | Select-Object -ExpandProperty Path
$OutputDir = Join-Path $BasePath "custom\output"

$Targets = @(
  "Draft-Keep.csv",
  "matched_channels.csv",
  "recommended_custom_list.csv",
  "consolidated_inventory.csv"
)

$bad = New-Object System.Collections.Generic.List[object]

foreach ($t in $Targets) {
  $p = Join-Path $OutputDir $t
  if (!(Test-Path $p)) { continue }

  $rows = Import-Csv $p
  $i = 0
  foreach ($r in $rows) {
    $i++

    $name = $r.name
    if (-not $name) { $name = $r.Channel }
    if (-not $name) { $name = $r."Channel Name" }

    $site = $r.site
    $xmltv = $r.xmltv_id

    if ([string]::IsNullOrWhiteSpace($name) -or
        [string]::IsNullOrWhiteSpace($site) -or
        [string]::IsNullOrWhiteSpace($xmltv)) {

      $bad.Add([pscustomobject]@{
        file=$t; row=$i; name=$name; site=$site; xmltv_id=$xmltv
      }) | Out-Null
    }
  }
}

if ($bad.Count -gt 0) {
  $badPath = Join-Path $OutputDir "ci_bad_rows.csv"
  $bad | Export-Csv -NoTypeInformation -Encoding UTF8 $badPath

  Write-Host "FOUND BAD ROWS. See custom/output/ci_bad_rows.csv"
  $bad | Format-Table -AutoSize | Out-String | Write-Host
  exit 1
}

Write-Host "OK: No bad rows detected."
exit 0
