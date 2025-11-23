<#
Script: enrich_draft_keep.ps1
Purpose:
  - Reads Draft-Keepcsv (your traceable working shortlist)
  - Auto-fills missing xmltv_id/site/site_id/lang from consolidated_inventory.csv
  - Writes Draft-Keep.enriched.csv + a "still_missing" report

Inputs:
  custom/output/Draft-Keep.csv
  custom/output/consolidated_inventory.csv

Outputs:
  custom/output/Draft-Keep.enriched.csv
  custom/output/Draft-Keep.still_missing.csv

Logs:
  custom/logs/enrich_draft_keep.log
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BasePath  = Resolve-Path (Join-Path $ScriptDir "..\..") | Select-Object -ExpandProperty Path
$OutputDir = Join-Path $BasePath "custom\output"
$LogDir    = Join-Path $BasePath "custom\logs"
$null = New-Item -ItemType Directory -Force -Path $OutputDir, $LogDir | Out-Null

$LogFile = Join-Path $LogDir "enrich_draft_keep.log"
function Write-Log([string]$msg,[string]$lvl="INFO"){
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line="[$ts][$lvl] $msg"
  Write-Host $line
  Add-Content -Path $LogFile -Value $line
}

function Norm([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return "" }
  $t = $s.ToLower()
  $t = $t -replace "\s*\(.*?\)\s*"," "
  $t = $t -replace "hd\b|uhd\b|\+1\b|timeshift\b"," "
  $t = $t -replace "[^a-z0-9]+"," "
  $t = ($t -replace "\s+"," ").Trim()
  return $t
}

$RawPath = Join-Path $OutputDir "Draft-Keep.csv"
$InvPath = Join-Path $OutputDir "consolidated_inventory.csv"
if(!(Test-Path $RawPath)){ throw "Missing $RawPath" }
if(!(Test-Path $InvPath)){ throw "Missing $InvPath" }

Write-Log "Loading keep list $RawPath"
$Raw = Import-Csv $RawPath
Write-Log "Loading inventory $InvPath"
$Inv = Import-Csv $InvPath

# Build lookup by normalized name + site_id fallback
$ByName = @{}
$BySiteId = @{}
foreach($r in $Inv){
  $nn = Norm $r.name
  if($nn -and -not $ByName.ContainsKey($nn)){ $ByName[$nn] = $r }
  if($r.site_id -and -not $BySiteId.ContainsKey($r.site_id)){ $BySiteId[$r.site_id] = $r }
}

$Enriched = New-Object System.Collections.Generic.List[object]
$StillMissing = New-Object System.Collections.Generic.List[object]

$i=0
foreach($r in $Raw){
  $i++

  $name = $r.name
  if(-not $name){ $name = $r.display_name }
  if(-not $name){ $name = $r.Channel }

  $site = $r.site
  $xmltv_id = $r.xmltv_id
  $site_id = $r.site_id
  $lang = $r.lang
  $source_file = $r.source_file

  $hit = $null

  if([string]::IsNullOrWhiteSpace($xmltv_id)){
    if($site_id -and $BySiteId.ContainsKey($site_id)){
      $hit = $BySiteId[$site_id]
    } else {
      $nn = Norm $name
      if($nn -and $ByName.ContainsKey($nn)){
        $hit = $ByName[$nn]
      }
    }
  }

  if($hit){
    if(-not $site){ $site = $hit.site }
    if(-not $xmltv_id){ $xmltv_id = $hit.xmltv_id }
    if(-not $site_id){ $site_id = $hit.site_id }
    if(-not $lang){ $lang = $hit.lang }
    if(-not $source_file){ $source_file = $hit.source_file }
  }

  $rowOut = [pscustomobject]@{
    name=$name
    site=$site
    xmltv_id=$xmltv_id
    lang=($lang ? $lang : "en")
    site_id=$site_id
    source_file=$source_file
  }

  $Enriched.Add($rowOut) | Out-Null

  if([string]::IsNullOrWhiteSpace($xmltv_id)){
    $StillMissing.Add([pscustomobject]@{
      row=$i; name=$name; site=$site; site_id=$site_id; source_file=$source_file
    }) | Out-Null
  }
}

$EnrichedPath = Join-Path $OutputDir "Draft-Keep.enriched.csv"
$MissingPath  = Join-Path $OutputDir "Draft-Keep.still_missing.csv"
$Enriched | Export-Csv -NoTypeInformation -Encoding UTF8 $EnrichedPath
$StillMissing | Export-Csv -NoTypeInformation -Encoding UTF8 $MissingPath

Write-Log "Wrote $EnrichedPath rows=$($Enriched.Count)"
Write-Log "Still missing xmltv_id rows=$($StillMissing.Count) -> $MissingPath"
Write-Log "Done."
