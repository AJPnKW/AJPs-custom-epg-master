<#
Script Name: step1_extract_all_sites_master_channels.ps1
Purpose:     Step 1 of AJP custom EPG pipeline. Extracts master channel list
             ONLY from whitelisted site XML files (step1_sources.csv).
Author:      ChatGPT (for Andrew J. Pearen)
Version:     3.1
Created:     2025-11-24
Last Update: 2025-11-24

Run:
  PowerShell: C:\Users\<user>\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step1_extract_all_sites_master_channels.ps1 -Mode Soft
#>

[CmdletBinding()]
param(
    [ValidateSet("Soft","Hard")]
    [string]$Mode = "Soft"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------
# Paths / constants
# ---------------------------
$ScriptName   = Split-Path $PSCommandPath -Leaf
$ScriptVer    = "3.1"
$NowStamp     = Get-Date -Format "yyyyMMdd_HHmmss"

$BasePath     = Resolve-Path (Join-Path $PSScriptRoot "..\..") | Select-Object -ExpandProperty Path
$CustomPath   = Join-Path $BasePath "custom"
$SitesPath    = Join-Path $BasePath "sites"
$RulesPath    = Join-Path $CustomPath "rules"
$LogsPath     = Join-Path $CustomPath "logs"
$BaselinePath = Join-Path $CustomPath "baseline"
$VersionedOut = Join-Path $BaselinePath "versioned"

$WhitelistCSV = Join-Path $RulesPath "step1_sources.csv"

# Output files
$StableCsv    = Join-Path $BaselinePath "all_sites_master_channels.csv"
$VersionedCsv = Join-Path $VersionedOut ("all_sites_master_channels_{0}.csv" -f $NowStamp)

# ---------------------------
# Safe folder ensures
# ---------------------------
$foldersToEnsure = @($LogsPath, $BaselinePath, $VersionedOut)
foreach ($f in $foldersToEnsure) {
    if (-not (Test-Path $f)) {
        New-Item -ItemType Directory -Force -Path $f | Out-Null
    }
}

# ---------------------------
# Logger with fallback if locked
# ---------------------------
function New-LogWriter {
    param([string]$DesiredPath)

    try {
        $sw = New-Object System.IO.StreamWriter($DesiredPath, $true, [System.Text.Encoding]::UTF8)
        $sw.AutoFlush = $true
        return @{ Writer=$sw; Path=$DesiredPath }
    }
    catch {
        # Fallback log if file is locked
        $fallback = [System.IO.Path]::Combine(
            (Split-Path $DesiredPath),
            ([System.IO.Path]::GetFileNameWithoutExtension($DesiredPath) + "_PID$PID.log")
        )
        $sw = New-Object System.IO.StreamWriter($fallback, $true, [System.Text.Encoding]::UTF8)
        $sw.AutoFlush = $true
        return @{ Writer=$sw; Path=$fallback }
    }
}

$LogFile = Join-Path $LogsPath "step1_extract_all_sites_master_channels.log"
$logObj  = New-LogWriter -DesiredPath $LogFile
$script:LogWriter = $logObj.Writer
$script:LogPath   = $logObj.Path

function Write-Log {
    param(
        [string]$Level,
        [string]$Message
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level.ToUpper(), $Message

    # console
    Write-Host $line

    # file (never crash pipeline on log failure)
    try { $script:LogWriter.WriteLine($line) } catch {}
}

Write-Log "INFO" "Starting $ScriptName v$ScriptVer (Mode=$Mode)"
Write-Log "INFO" "BasePath=$BasePath"
Write-Log "INFO" "SitesPath=$SitesPath"
Write-Log "INFO" "WhitelistCSV=$WhitelistCSV"
Write-Log "DEBUG" "LogPath=$script:LogPath"

# ---------------------------
# Load whitelist
# ---------------------------
if (-not (Test-Path $WhitelistCSV)) {
    Write-Log "ERROR" "Whitelist file not found: $WhitelistCSV"
    if ($Mode -eq "Hard") { throw "Whitelist missing." } else { exit 1 }
}

$WhitelistRaw = Import-Csv $WhitelistCSV
$Whitelist    = @($WhitelistRaw)  # force array to avoid Count regression
$WhitelistCnt = ($Whitelist | Measure-Object).Count
Write-Log "INFO" "Whitelist entries loaded: $WhitelistCnt"

$requiredCols = @("site","enabled","allowed_files","note","default_country")
$missing = @()
foreach ($col in $requiredCols) {
    if (-not ($WhitelistRaw | Get-Member -Name $col)) { $missing += $col }
}
if ($missing.Count -gt 0) {
    Write-Log "ERROR" ("Whitelist missing columns: {0}" -f ($missing -join ", "))
    if ($Mode -eq "Hard") { throw "Whitelist schema invalid." } else { exit 1 }
}

# keep only enabled=true rows
$WhitelistEnabled = @($Whitelist | Where-Object {
    $_.enabled -match '^(true|1|yes)$'
})
Write-Log "INFO" ("Enabled whitelist rows: {0}" -f (($WhitelistEnabled | Measure-Object).Count))

# ---------------------------
# Helper: parse XMLTV without namespace issues
# ---------------------------
function Get-TvNode {
    param([xml]$XmlDoc)

    if ($XmlDoc.tv) { return $XmlDoc.tv }

    if ($XmlDoc.DocumentElement -and $XmlDoc.DocumentElement.LocalName -eq "tv") {
        return $XmlDoc.DocumentElement
    }

    # fallback for namespaces
    return $XmlDoc.SelectSingleNode('//*[local-name()="tv"]')
}

function Get-ChannelNodes {
    param($TvNode)
    if (-not $TvNode) { return @() }

    # local-name() handles namespaces
    return @($TvNode.SelectNodes('./*[local-name()="channel"]'))
}

# ---------------------------
# Queue files
# ---------------------------
$queue = New-Object System.Collections.Generic.List[object]

foreach ($row in $WhitelistEnabled) {
    $site = ($row.site | ForEach-Object { $_.Trim() })
    $allowed = ($row.allowed_files | ForEach-Object { $_.Trim() })

    if ([string]::IsNullOrWhiteSpace($site) -or [string]::IsNullOrWhiteSpace($allowed)) {
        Write-Log "WARN" "Whitelist row missing site/allowed_files: $($row | ConvertTo-Json -Compress)"
        continue
    }

    # allow multiple allowed_files separated by ; or |
    $files = $allowed -split '[;|]' | ForEach-Object { $_.Trim() } | Where-Object { $_ }

    foreach ($relFile in $files) {
        $full = Join-Path (Join-Path $SitesPath $site) $relFile
        $queue.Add([pscustomobject]@{
            site            = $site
            relative_file   = $relFile
            full_path       = $full
            note            = $row.note
            default_country = $row.default_country
        })
    }
}

Write-Log "INFO" ("Files queued: {0}" -f $queue.Count)
if ($queue.Count -eq 0) {
    Write-Log "ERROR" "No files queued. Check enabled and allowed_files in step1_sources.csv."
    if ($Mode -eq "Hard") { throw "No files queued." } else { exit 1 }
}

# ---------------------------
# Extract
# ---------------------------
$AllRows = New-Object System.Collections.Generic.List[object]
$filesProcessed = 0

foreach ($item in $queue) {
    $relDisplay = Join-Path $item.site $item.relative_file

    if (-not (Test-Path $item.full_path)) {
        Write-Log "WARN" "Missing file: $relDisplay"
        if ($Mode -eq "Hard") { throw "Missing file $relDisplay" }
        continue
    }

    Write-Log "INFO" "Loading: $relDisplay"

    try {
        $raw = Get-Content -Raw -LiteralPath $item.full_path
        if ([string]::IsNullOrWhiteSpace($raw)) {
            throw "Empty XML file."
        }

        $xml = [xml]$raw
        $tvNode = Get-TvNode -XmlDoc $xml
        if (-not $tvNode) {
            throw "No <tv> root found (namespace or invalid XMLTV)."
        }

        $channels = Get-ChannelNodes -TvNode $tvNode
        if ($channels.Count -eq 0) {
            Write-Log "WARN" "No <channel> nodes found in $relDisplay"
            $filesProcessed++
            continue
        }

        foreach ($ch in $channels) {
            $idAttr = $ch.id
            if ([string]::IsNullOrWhiteSpace($idAttr)) { $idAttr = "" }

            $displayNodes = @($ch.SelectNodes('./*[local-name()="display-name"]'))
            $name = ""
            $callsign = ""
            $lang = ""

            if ($displayNodes.Count -ge 1) {
                $name = ($displayNodes[0].InnerText).Trim()
                $lang = ($displayNodes[0].GetAttribute("lang")).Trim()
            }
            if ($displayNodes.Count -ge 2) {
                $callsign = ($displayNodes[1].InnerText).Trim()
            }

            $countryNode = $ch.SelectSingleNode('./*[local-name()="country"]')
            $country = ""
            if ($countryNode) {
                $country = ($countryNode.InnerText).Trim()
            }
            if ([string]::IsNullOrWhiteSpace($country)) {
                $country = ($item.default_country).Trim()  # may be blank for GLOBAL
            }

            # basic quality/resolution if present in some feeds
            $resNode = $ch.SelectSingleNode('./*[local-name()="video"]/*[local-name()="quality"]')
            $quality = if ($resNode) { ($resNode.InnerText).Trim() } else { "" }

            $AllRows.Add([pscustomobject]@{
                site          = $item.site
                relative_file = $relDisplay
                channel_name  = $name
                callsign      = $callsign
                lang          = $lang
                channel_id    = $idAttr
                country       = $country
                quality       = $quality
            })
        }

        $filesProcessed++
    }
    catch {
        Write-Log "ERROR" "Failed parsing $relDisplay : $($_.Exception.Message)"
        if ($Mode -eq "Hard") { throw }
        continue
    }
}

Write-Log "INFO" ("Files processed: {0}" -f $filesProcessed)
Write-Log "INFO" ("Rows collected (raw): {0}" -f $AllRows.Count)

# ---------------------------
# Deduplicate
# ---------------------------
$deduped = @(
    $AllRows |
    Sort-Object site, relative_file, channel_id, channel_name, callsign, lang, country, quality -Unique
)
$dupRemoved = $AllRows.Count - $deduped.Count
Write-Log "INFO" ("Deduped rows: {0} (duplicates removed={1})" -f $deduped.Count, $dupRemoved)

# ---------------------------
# Write outputs
# ---------------------------
try {
    $deduped | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $StableCsv -Force
    Write-Log "OK" "Wrote stable baseline CSV: $StableCsv"

    $deduped | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $VersionedCsv -Force
    Write-Log "OK" "Wrote versioned CSV: $VersionedCsv"
}
catch {
    Write-Log "ERROR" "Failed writing output CSV(s): $($_.Exception.Message)"
    if ($Mode -eq "Hard") { throw }
}

Write-Log "INFO" "Step 1 complete."
Write-Log "INFO" "Finished $ScriptName"

try { $script:LogWriter.Flush(); $script:LogWriter.Close() } catch {}
