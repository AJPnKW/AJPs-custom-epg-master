<#
Script Name: merge_final_custom_channels_v6.ps1
Purpose: Merge local channel inventories + your keep lists into a final custom.channels XML.
         Local-only. No grabbing. Handles nulls safely. Produces audit CSVs.
Author: ChatGPT for Andrew Pearen
Creation Date: 2025-11-22
Last Update Date: 2025-11-22
Version: 6.0

Execution:
# PowerShell:
C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\merge_final_custom_channels_v6.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------
# Paths / folders
# ---------------------------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BasePath  = Resolve-Path (Join-Path $ScriptDir "..\..") | Select-Object -ExpandProperty Path
$CustomDir = Join-Path $BasePath "custom"
$OutputDir = Join-Path $CustomDir "output"
$LogDir    = Join-Path $CustomDir "logs"

$null = New-Item -ItemType Directory -Force -Path $OutputDir, $LogDir | Out-Null

$NowStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$LogFile  = Join-Path $LogDir "merge_final_custom_channels_v6.log"

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("INFO","WARN","DEBUG","ERROR","TRACE")][string]$Level="INFO"
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts][$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

Write-Log "Starting merge_final_custom_channels_v6.ps1"
Write-Log "BasePath=$BasePath"
Write-Log "OutputPath=$OutputDir"
Write-Log "LogPath=$LogDir"

# ---------------------------
# Inputs (local-only)
# ---------------------------
$InventoryCsv         = Join-Path $OutputDir "consolidated_inventory.csv"
$DedupedXml           = Join-Path $OutputDir "custom.channels.deduped.v2.xml"
$MergedXmlOptional    = Join-Path $OutputDir "custom.channels.merged.xml"
$CustomBaseXmlOptional= Join-Path $OutputDir "custom.channels.xml"
$MatchedCsvOptional   = Join-Path $OutputDir "matched_channels.csv"
$RecommendedCsvOptional = Join-Path $OutputDir "recommended_custom_list.csv"
$DraftKeepCsvOptional = Join-Path $OutputDir "Draft-Keep.csv"

if (-not (Test-Path $InventoryCsv)) {
    throw "Missing required input: $InventoryCsv"
}

# ---------------------------
# Site priority (Option 1 winner order)
# smaller number = better
# ---------------------------
$SitePriority = @{
    "epgshare01.online"          = 1
    "directv.com"               = 2
    "freeview.co.uk"            = 2
    "sky.com"                   = 3
    "virgintvgo.virginmedia.com"= 3
    "tv24.co.uk"                = 4
    "tvpassport.com"            = 4
    "tvtv.us"                    = 4
    "tvguide.com"               = 5
    "tvinsider.com"             = 5
    "pluto.tv"                  = 6
    "plex.tv"                   = 6
    "abc.net.au"                = 6
    "streamingtvguides.com"     = 7
    "player.ee.co.uk"           = 7
}

function Get-Priority([string]$site) {
    if ([string]::IsNullOrWhiteSpace($site)) { return 99 }
    $s = $site.ToLower()
    if ($SitePriority.ContainsKey($s)) { return [int]$SitePriority[$s] }
    return 50
}

# ---------------------------
# Helpers
# ---------------------------
function Normalize-Name([string]$name) {
    if ([string]::IsNullOrWhiteSpace($name)) { return "" }
    $n = $name.ToLower()

    # strip common junk
    $n = $n -replace "\s*\(.*?\)\s*", " "        # drop bracketed callsigns/cities
    $n = $n -replace "hd\b", ""
    $n = $n -replace "uhd\b", ""
    $n = $n -replace "\+1\b", ""
    $n = $n -replace "timeshift\b", ""
    $n = $n -replace "east(ern)?\b", " east"
    $n = $n -replace "west(ern)?\b", " west"
    $n = $n -replace "[^a-z0-9]+", " "          # keep alnum
    $n = ($n -replace "\s+", " ").Trim()
    return $n
}

function Is-Uk-Variant([string]$name) {
    if ([string]::IsNullOrWhiteSpace($name)) { return $false }
    $n = $name.ToLower()
    return ($n -match "\+1\b" -or $n -match "timeshift" -or $n -match "region" -or $n -match "scotland|wales|ni|london|midlands|yorkshire")
}

# UK-A2: keep ONE primary per network
function Uk-Network-Key([string]$name) {
    $n = Normalize-Name $name
    # BBC One / Two etc collapse to "bbc one", "bbc two"
    if ($n -match "^bbc\s+(one|two|three|four|news|parliament)") { return $Matches[0] }
    if ($n -match "^itv\s*\d?") { return "itv" }
    if ($n -match "^channel\s+4") { return "channel 4" }
    if ($n -match "^channel\s+5") { return "channel 5" }
    if ($n -match "^sky\s+") { return ($n -replace "\s+.*$", "") } # "sky" family grouped
    return $n
}

function Load-Xml-Channels([string]$path, [string]$sourceTag) {
    $list = @()
    if (-not (Test-Path $path)) { 
        Write-Log "Optional XML not found: $path" "WARN"
        return $list
    }
    try {
        [xml]$x = Get-Content -LiteralPath $path -Raw
        foreach ($ch in $x.channels.channel) {
            $list += [pscustomobject]@{
                source   = $sourceTag
                name     = [string]$ch.'#text'
                site     = [string]$ch.site
                xmltv_id = [string]$ch.xmltv_id
            }
        }
        Write-Log "Loaded XML $sourceTag count=$($list.Count)"
    } catch {
        Write-Log "Failed loading XML $path: $_" "ERROR"
    }
    return $list
}

function Load-Csv([string]$path, [string]$sourceTag) {
    $list = @()
    if (-not (Test-Path $path)) {
        Write-Log "Optional CSV not found: $path" "WARN"
        return $list
    }
    try {
        $rows = Import-Csv -LiteralPath $path
        foreach ($r in $rows) {
            $list += [pscustomobject]@{
                source   = $sourceTag
                name     = [string]$r.name
                site     = [string]$r.site
                xmltv_id = [string]$r.xmltv_id
            }
        }
        Write-Log "Loaded CSV $sourceTag count=$($list.Count)"
    } catch {
        Write-Log "Failed loading CSV $path: $_" "ERROR"
    }
    return $list
}

# ---------------------------
# Load inventory + optional keep sources
# ---------------------------
$Inventory = Import-Csv -LiteralPath $InventoryCsv
Write-Log "Loaded consolidated_inventory.csv rows=$($Inventory.Count)"

$Seeds = @()
$Seeds += Load-Xml-Channels $DedupedXml "deduped.v2"
$Seeds += Load-Xml-Channels $MergedXmlOptional "merged"
$Seeds += Load-Xml-Channels $CustomBaseXmlOptional "custom.base"
$Seeds += Load-Csv $MatchedCsvOptional "matched"
$Seeds += Load-Csv $RecommendedCsvOptional "recommended"
$Seeds += Load-Csv $DraftKeepCsvOptional "draftkeep"

# If no seeds at all, fall back to inventory itself
if ($Seeds.Count -eq 0) {
    Write-Log "No seed lists found; using entire inventory as candidates" "WARN"
    foreach ($r in $Inventory) {
        $Seeds += [pscustomobject]@{
            source   = "inventory"
            name     = [string]$r.name
            site     = [string]$r.site
            xmltv_id = [string]$r.xmltv_id
        }
    }
}

# ---------------------------
# Candidate validation + keying
# ---------------------------
$BadRows = New-Object System.Collections.Generic.List[object]
$Candidates = New-Object System.Collections.Generic.List[object]

foreach ($ch in $Seeds) {
    $rawName = $ch.name
    $rawSite = $ch.site
    $rawId   = $ch.xmltv_id

    if ([string]::IsNullOrWhiteSpace($rawName)) {
        $BadRows.Add([pscustomobject]@{ source=$ch.source; reason="missing_name"; name=$rawName; site=$rawSite; xmltv_id=$rawId })
        Write-Log "[MISSING_NAME] source=$($ch.source) name='' site='$rawSite' xmltv_id='$rawId'" "WARN"
        continue
    }

    if ([string]::IsNullOrWhiteSpace($rawSite)) {
        $BadRows.Add([pscustomobject]@{ source=$ch.source; reason="missing_site"; name=$rawName; site=$rawSite; xmltv_id=$rawId })
        Write-Log "[MISSING_SITE] source=$($ch.source) name='$rawName' site='' xmltv_id='$rawId'" "WARN"
        continue
    }

    $norm = Normalize-Name $rawName
    if ([string]::IsNullOrWhiteSpace($norm)) {
        $BadRows.Add([pscustomobject]@{ source=$ch.source; reason="bad_normalization"; name=$rawName; site=$rawSite; xmltv_id=$rawId })
        Write-Log "[BAD_NORMALIZATION] source=$($ch.source) name='$rawName'" "WARN"
        continue
    }

    $Candidates.Add([pscustomobject]@{
        source   = $ch.source
        name     = $rawName
        norm     = $norm
        site     = $rawSite.ToLower()
        xmltv_id = $rawId
        priority = Get-Priority $rawSite
        uk_key   = if ($rawSite.ToLower() -match "freeview\.co\.uk|sky\.com|tv24\.co\.uk|virgintvgo\.virginmedia\.com|epgshare01\.online") { Uk-Network-Key $rawName } else { "" }
        uk_variant = if ($rawSite.ToLower() -match "freeview\.co\.uk|sky\.com|tv24\.co\.uk|virgintvgo\.virginmedia\.com|epgshare01\.online") { Is-Uk-Variant $rawName } else { $false }
    })
}

Write-Log "Total valid candidates=$($Candidates.Count)"
if ($BadRows.Count -gt 0) {
    $BadRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path (Join-Path $OutputDir "merge_skipped_bad_rows.v6.csv")
    Write-Log "Wrote merge_skipped_bad_rows.v6.csv rows=$($BadRows.Count)"
}

# ---------------------------
# Merge / dedupe
# Key logic:
# - General: key by norm + country-ish xmltv id if present
# - UK-A2: collapse by uk_key, prefer non-variant primary
# ---------------------------
$FinalMap = @{}
$Dupes = New-Object System.Collections.Generic.List[object]

foreach ($ch in $Candidates) {

    $key = $ch.norm
    if (-not [string]::IsNullOrWhiteSpace($ch.uk_key)) {
        $key = "uk|" + $ch.uk_key
    }

    if (-not $FinalMap.ContainsKey($key)) {
        $FinalMap[$key] = $ch
        continue
    }

    $prev = $FinalMap[$key]

    $win = $false

    # 1) priority wins
    if ($ch.priority -lt $prev.priority) {
        $win = $true
        Write-Log "[TRACE][WIN] site priority ${($ch.priority)}<${($prev.priority)}: new wins for key=$key" "DEBUG"
    }
    elseif ($ch.priority -gt $prev.priority) {
        $win = $false
    }
    else {
        # 2) UK-A2 prefer primary (non-variant)
        if ($key.StartsWith("uk|")) {
            if ($prev.uk_variant -and -not $ch.uk_variant) { $win = $true }
            elseif (-not $prev.uk_variant -and $ch.uk_variant) { $win = $false }
        }

        # 3) if still tie, prefer one with xmltv_id
        if (-not $win) {
            $prevHasId = -not [string]::IsNullOrWhiteSpace($prev.xmltv_id)
            $newHasId  = -not [string]::IsNullOrWhiteSpace($ch.xmltv_id)
            if ($newHasId -and -not $prevHasId) { $win = $true }
        }
    }

    if ($win) {
        $Dupes.Add([pscustomobject]@{ key=$key; kept=$ch.name; dropped=$prev.name; kept_site=$ch.site; dropped_site=$prev.site })
        $FinalMap[$key] = $ch
    }
    else {
        $Dupes.Add([pscustomobject]@{ key=$key; kept=$prev.name; dropped=$ch.name; kept_site=$prev.site; dropped_site=$ch.site })
    }
}

# ---------------------------
# Write outputs
# ---------------------------
$FinalList = $FinalMap.Values | Sort-Object site, name
Write-Log "Final unique channels=$($FinalList.Count)"
Write-Log "Duplicates collapsed=$($Dupes.Count)"

$Dupes | Export-Csv -NoTypeInformation -Encoding UTF8 -Path (Join-Path $OutputDir "merge_review_duplicates.v6.csv")
Write-Log "Wrote merge_review_duplicates.v6.csv"

# XML
$xmlOutPath = Join-Path $OutputDir "custom.channels.final.v6.xml"

$sb = New-Object System.Text.StringBuilder
[void]$sb.AppendLine("<?xml version='1.0' encoding='utf-8'?>")
[void]$sb.AppendLine("<channels>")

foreach ($ch in $FinalList) {
    $safeName = [System.Security.SecurityElement]::Escape($ch.name)
    if (-not [string]::IsNullOrWhiteSpace($ch.xmltv_id)) {
        [void]$sb.AppendLine("  <channel site=""$($ch.site)"" xmltv_id=""$($ch.xmltv_id)"">$safeName</channel>")
    } else {
        [void]$sb.AppendLine("  <channel site=""$($ch.site)"">$safeName</channel>")
    }
}

[void]$sb.AppendLine("</channels>")
Set-Content -Encoding UTF8 -LiteralPath $xmlOutPath -Value $sb.ToString()
Write-Log "Wrote $xmlOutPath"

# CSV
$csvOutPath = Join-Path $OutputDir "custom.channels.final.v6.csv"
$FinalList | Select-Object site, xmltv_id, name, source, priority, norm |
    Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvOutPath
Write-Log "Wrote $csvOutPath"

Write-Log "Done."
