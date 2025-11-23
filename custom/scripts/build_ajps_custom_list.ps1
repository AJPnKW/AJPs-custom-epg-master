<#
Script Name: build_ajps_custom_list.ps1
Purpose    : Build AJPs_Custom_Channel_list.csv + AJPs_Custom_Channel_list.xml from keep_seeds.csv + consolidated inventory + Draft-Keep.enriched.csv
Author     : ChatGPT for Andrew Pearen
Created    : 2025-11-23
Updated    : 2025-11-23
Version    : 2.0
Run:
PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\build_ajps_custom_list.ps1
Inputs:
custom\data\keep_seeds.csv
custom\output\Draft-Keep.enriched.csv   (or Draft-Keep.csv fallback)
custom\output\consolidated_inventory.csv
Outputs:
custom\output\AJPs_Custom_Channel_list.csv
custom\output\AJPs_Custom_Channel_list.xml
custom\output\ci_bad_rows.csv
Notes:
- LOCAL FILES ONLY.
- Blank country in seeds is auto-inferred (from xmltv suffix or site tag). If still unknown -> logged, skipped.
- Prefers SD/non-HD when multiple candidates exist.
- Adds optional backups if they exist.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
# ---------- paths ----------
$BasePath   = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$DataPath   = Join-Path $BasePath "custom\data"
$OutputPath = Join-Path $BasePath "custom\output"
$LogPath    = Join-Path $BasePath "custom\logs"
$SeedsPath  = Join-Path $DataPath "keep_seeds.csv"
$InvPath    = Join-Path $OutputPath "consolidated_inventory.csv"
$KeepPath1  = Join-Path $OutputPath "Draft-Keep.enriched.csv"
$KeepPath2  = Join-Path $OutputPath "Draft-Keep.csv"
$OutCsv     = Join-Path $OutputPath "AJPs_Custom_Channel_list.csv"
$OutXml     = Join-Path $OutputPath "AJPs_Custom_Channel_list.xml"
$BadRowsOut = Join-Path $OutputPath "ci_bad_rows.csv"
$LogFile    = Join-Path $LogPath "build_ajps_custom_list.log"
New-Item -ItemType Directory -Force -Path $DataPath, $OutputPath, $LogPath | Out-Null
function Write-Log {
param([string]$Msg, [string]$Level="INFO")
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$line = "[$ts][$Level] $Msg"
Add-Content -Path $LogFile -Value $line
Write-Host $line
}
Write-Log "Starting build_ajps_custom_list.ps1 v2.0"
Write-Log "BasePath=$BasePath"
if(!(Test-Path $SeedsPath)){ throw "Missing $SeedsPath (create it once)" }
if(!(Test-Path $InvPath)){ throw "Missing $InvPath" }
$KeepPath = (Test-Path $KeepPath1) ? $KeepPath1 : $KeepPath2
if(!(Test-Path $KeepPath)){ throw "Missing keep file. Expected Draft-Keep.enriched.csv or Draft-Keep.csv in $OutputPath" }
$seeds = Import-Csv $SeedsPath
$inv   = Import-Csv $InvPath
$keep  = Import-Csv $KeepPath
Write-Log "Seeds=$($seeds.Count) inventory=$($inv.Count) keep=$($keep.Count)"
# ---------- helpers ----------
function Norm([string]$s){
if(-not $s){ return "" }
return ($s.ToLower() -replace '[^a-z0-9]+',' ').Trim()
}
function Infer-Country($seed){
$c = ($seed.country ?? "").Trim()
if($c){ return $c.ToUpper() }
$xml = ($seed.xmltv_id ?? "").Trim()
if($xml -match '\.ca@' -or $xml -match '\.ca$'){ return "CA" }
if($xml -match '\.us@' -or $xml -match '\.us$'){ return "US" }
if($xml -match '\.uk@' -or $xml -match '\.uk$'){ return "UK" }
if($xml -match '\.au@' -or $xml -match '\.au$'){ return "AU" }
$site = ($seed.site ?? "").ToLower()
if($site -match 'abc\.net\.au'){ return "AU" }
if($site -match 'tv24\.co\.uk|virginmedia|freeview|sky\.com'){ return "UK" }
return $null
}
function Is-HD([string]$name){
if(-not $name){ return $false }
return ($name -match '\bHD\b' -or $name -match '\bH\.D\.\b')
}
# ---------- inventory lookup ----------
$invByCountryName = @{}
foreach($r in $inv){
$country = (($r.country ?? "") -replace '\s+','').ToUpper()
$nameN   = Norm $r.name
if($country -and $nameN){
$k = "$country|$nameN"
if(-not $invByCountryName.ContainsKey($k)){
$invByCountryName[$k] = New-Object System.Collections.Generic.List[object]
}
$invByCountryName[$k].Add($r) | Out-Null
}
}
# ---------- build list ----------
$badRows = New-Object System.Collections.Generic.List[object]
$outRows = New-Object System.Collections.Generic.List[object]
foreach($seed in $seeds){
$display = ($seed.display_name ?? "").Trim()
$hint    = ($seed.match_hint ?? "").Trim()
$country = Infer-Country $seed
if(-not $display -or -not $hint -or -not $country){
$badRows.Add($seed) | Out-Null
Write-Log "[BAD_SEED] display='$display' hint='$hint' country='$($seed.country)'" "WARN"
continue
}
$key = "$country|$(Norm $hint)"
$cands = $invByCountryName[$key]
if(-not $cands){
Write-Log "[NO_MATCH] seed='$display' hint='$hint' country=$country" "WARN"
continue
}
# prefer SD/non-HD, then site priority
$preferred = $cands | Sort-Object `
@{Expression={Is-HD $_.name}; Ascending=$true}, `
@{Expression={$_.site_priority ?? 9}; Ascending=$true}
$primary = $preferred[0]
# backups: take next distinct sites if any
$backups = @()
foreach($c in $preferred){
if($c.site -ne $primary.site){
$backups += $c
}
if($backups.Count -ge 2){ break }
}
# build naming like: "CTV Toronto [CA] (epgshare)"
function Make-Name($base,$ct,$site){
$ctag = "[$ct]"
$s = ($site -replace '^www\.','')
"$base $ctag ($s)"
}
$outRows.Add([pscustomobject]@{
role        = "primary"
display_name= (Make-Name $display $country $primary.site)
xmltv_id    = $primary.xmltv_id
site        = $primary.site
site_id     = $primary.site_id
source_file = $primary.source_file
}) | Out-Null
$i=1
foreach($b in $backups){
$outRows.Add([pscustomobject]@{
role        = "backup$($i)"
display_name= (Make-Name $display $country $b.site)
xmltv_id    = $b.xmltv_id
site        = $b.site
site_id     = $b.site_id
source_file = $b.source_file
}) | Out-Null
$i++
}
}
# write CSV
$outRows | Export-Csv $OutCsv -NoTypeInformation -Encoding UTF8
Write-Log "Wrote $OutCsv rows=$($outRows.Count)"
# write XML (simple channels only)
$xml = New-Object System.Xml.XmlDocument
$root = $xml.CreateElement("channels")
$xml.AppendChild($root) | Out-Null
foreach($r in $outRows){
if(-not $r.xmltv_id){ continue } # xml requires id
$c = $xml.CreateElement("channel")
$c.SetAttribute("name",$r.display_name)
$c.SetAttribute("xmltv_id",$r.xmltv_id)
$c.SetAttribute("site",$r.site)
$c.SetAttribute("site_id",$r.site_id)
$root.AppendChild($c) | Out-Null
}
$xml.Save($OutXml)
Write-Log "Wrote $OutXml channels=$($root.ChildNodes.Count)"
# bad rows
if($badRows.Count -gt 0){
$badRows | Export-Csv $BadRowsOut -NoTypeInformation -Encoding UTF8
Write-Log "FOUND BAD SEED ROWS -> $BadRowsOut count=$($badRows.Count)" "WARN"
} else {
if(Test-Path $BadRowsOut){ Remove-Item $BadRowsOut -Force }
Write-Log "No bad seed rows."
}
Write-Log "Done."
