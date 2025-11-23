<#
Script Name: enrich_draft_keep_raw.ps1
Purpose    : Enrich Draft-Keep keep-list with xmltv_id + metadata from consolidated_inventory.csv
Author     : ChatGPT for Andrew Pearen
Created    : 2025-11-23
Updated    : 2025-11-23
Version    : 2.0
Run:
PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\enrich_draft_keep_raw.ps1
Notes:
- LOCAL FILES ONLY. No internet calls.
- Accepts Draft-Keep.csv as default.
- If Draft-Keep.raw.csv exists, it is used (backcompat).
Outputs:
- Draft-Keep.enriched.csv
- Draft-Keep.still_missing.csv (rows still missing xmltv_id)
Logs:
- custom\logs\enrich_draft_keep.log
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
# ---------- paths ----------
$BasePath   = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$OutputPath = Join-Path $BasePath "custom\output"
$LogPath    = Join-Path $BasePath "custom\logs"
$RawPath1   = Join-Path $OutputPath "Draft-Keep.raw.csv"
$RawPath2   = Join-Path $OutputPath "Draft-Keep.csv"
$InvPath    = Join-Path $OutputPath "consolidated_inventory.csv"
$EnrichedOut = Join-Path $OutputPath "Draft-Keep.enriched.csv"
$MissingOut  = Join-Path $OutputPath "Draft-Keep.still_missing.csv"
$LogFile     = Join-Path $LogPath "enrich_draft_keep.log"
New-Item -ItemType Directory -Force -Path $OutputPath, $LogPath | Out-Null
function Write-Log {
param([string]$Msg, [string]$Level="INFO")
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$line = "[$ts][$Level] $Msg"
Add-Content -Path $LogFile -Value $line
Write-Host $line
}
Write-Log "Starting enrich_draft_keep_raw.ps1 v2.0"
Write-Log "BasePath=$BasePath"
Write-Log "OutputPath=$OutputPath"
# ---------- choose keep file ----------
$KeepPath = $null
if(Test-Path $RawPath1){ $KeepPath = $RawPath1 }
elseif(Test-Path $RawPath2){ $KeepPath = $RawPath2 }
else { throw "Missing keep list. Expected Draft-Keep.csv (or Draft-Keep.raw.csv) in $OutputPath" }
if(!(Test-Path $InvPath)){ throw "Missing inventory $InvPath" }
Write-Log "Loading keep list $KeepPath"
$keep = Import-Csv $KeepPath
Write-Log "Loading inventory $InvPath"
$inv = Import-Csv $InvPath
# inventory lookup maps
# prefer exact match by site+site_id, then xmltv_id, then name within same site
$invBySiteId = @{}
$invByXmltv  = @{}
$invByName   = @{}
foreach($r in $inv){
$site = ($r.site ?? "").Trim()
$siteId = ($r.site_id ?? "").Trim()
$xml = ($r.xmltv_id ?? "").Trim()
$name = ($r.name ?? "").Trim()
if($site -and $siteId){
$k = "$site|$siteId"
if(-not $invBySiteId.ContainsKey($k)){ $invBySiteId[$k] = $r }
}
if($xml){
if(-not $invByXmltv.ContainsKey($xml)){ $invByXmltv[$xml] = $r }
}
if($site -and $name){
$nk = "$site|$($name.ToLower())"
if(-not $invByName.ContainsKey($nk)){ $invByName[$nk] = $r }
}
}
$enriched = New-Object System.Collections.Generic.List[object]
$stillMissing = New-Object System.Collections.Generic.List[object]
foreach($k in $keep){
$site    = ($k.site ?? $k.tag ?? "").Trim()
$site_id = ($k.site_id ?? "").Trim()
$name    = ($k.name ?? "").Trim()
$xmltv   = ($k.xmltv_id ?? "").Trim()
$lang    = ($k.lang ?? "").Trim()
$srcfile = ($k.source_file ?? "").Trim()
$match = $null
if($site -and $site_id){
$key = "$site|$site_id"
$match = $invBySiteId[$key]
}
if(-not $match -and $xmltv){
$match = $invByXmltv[$xmltv]
}
if(-not $match -and $site -and $name){
$nk = "$site|$($name.ToLower())"
$match = $invByName[$nk]
}
if($match){
if(-not $xmltv){ $xmltv = ($match.xmltv_id ?? "").Trim() }
if(-not $lang){  $lang  = ($match.lang ?? "").Trim() }
$row = [pscustomobject]@{
tag         = ($k.tag ?? $site)
site        = $site
name        = $name
xmltv_id    = $xmltv
lang        = $lang
site_id     = $site_id
source_file = ($match.source_file ?? $srcfile)
}
} else {
$row = [pscustomobject]@{
tag         = ($k.tag ?? $site)
site        = $site
name        = $name
xmltv_id    = $xmltv
lang        = $lang
site_id     = $site_id
source_file = $srcfile
}
}
$enriched.Add($row) | Out-Null
if(-not $row.xmltv_id){ $stillMissing.Add($row) | Out-Null }
}
$enriched | Export-Csv $EnrichedOut -NoTypeInformation -Encoding UTF8
$stillMissing | Export-Csv $MissingOut -NoTypeInformation -Encoding UTF8
Write-Log "Wrote $EnrichedOut rows=$($enriched.Count)"
Write-Log "Still missing xmltv_id rows=$($stillMissing.Count) -> $MissingOut"
Write-Log "Done."
