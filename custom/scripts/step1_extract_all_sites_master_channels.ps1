<#
-------------------------------------------------------------------------------
Script Name: step1_extract_all_sites_master_channels.ps1
Purpose    : STEP 1 of AJP Custom EPG pipeline.
             Extract channel master list ONLY from approved/whitelisted
             site channel XML files, producing a consolidated CSV baseline.

Author     : ChatGPT for Andrew Pearen
Version    : 3.0
Created    : 2025-11-23
Updated    : 2025-11-23

Run Cmd    : PowerShell:
             C:\Users\<user>\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step1_extract_all_sites_master_channels.ps1

Inputs     :
  - Whitelist CSV (optional, auto-created if missing):
      custom\rules\step1_sources.csv
    If missing, internal defaults are used.

  - Approved XML files under:
      sites\<site>\*.channels.xml

Outputs    :
  - Master CSV:
      custom\data\all_sites_master_channels.csv
  - Versioned Master CSV:
      custom\data\versioned-master\master_channels_yyyyMMdd_HHmmss.csv
  - Log:
      custom\logs\step1_extract_all_sites_master_channels.log

Notes:
  - No recursive scan of sites/. Only whitelist files are loaded.
  - Robust name extraction (InnerText, @name, <display-name>).
  - Safe log append with retry to avoid file locks.
-------------------------------------------------------------------------------
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------[ Paths & Setup ]---------------------------------
$ScriptVersion = "3.0"
$StartStamp    = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$StampCompact  = Get-Date -Format "yyyyMMdd_HHmmss"

$ScriptRoot = Split-Path -Parent $PSCommandPath
$CustomPath = Split-Path -Parent $ScriptRoot
$BasePath   = Split-Path -Parent $CustomPath

$SitesPath  = Join-Path $BasePath "sites"
$DataPath   = Join-Path $CustomPath "data"
$RulesPath  = Join-Path $CustomPath "rules"
$LogsPath   = Join-Path $CustomPath "logs"

$MasterCsv  = Join-Path $DataPath "all_sites_master_channels.csv"
$VersionDir = Join-Path $DataPath "versioned-master"
$VersionCsv = Join-Path $VersionDir ("master_channels_{0}.csv" -f $StampCompact)

$LogFile    = Join-Path $LogsPath "step1_extract_all_sites_master_channels.log"
$SourcesCsv = Join-Path $RulesPath "step1_sources.csv"

$RequiredDirs = @($DataPath, $RulesPath, $LogsPath, $VersionDir)
foreach ($d in $RequiredDirs) {
    if (!(Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

# ---------------------------[ Logging ]---------------------------------------
function Write-Log {
    param(
        [Parameter(Mandatory=$true)][ValidateSet("INFO","OK","WARN","ERROR","DEBUG")]$Level,
        [Parameter(Mandatory=$true)][string]$Message
    )

    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level, $Message

    # Console: INFO/OK/WARN/ERROR only (clean)
    if ($Level -ne "DEBUG") {
        Write-Host $line
    }

    # File: all levels, safe append with retry
    $maxTries = 5
    for ($i=1; $i -le $maxTries; $i++) {
        try {
            Add-Content -Path $LogFile -Value $line -Encoding UTF8
            break
        } catch {
            if ($i -eq $maxTries) { throw }
            Start-Sleep -Milliseconds (150 * $i)
        }
    }
}

Write-Log "INFO" "Starting step1_extract_all_sites_master_channels.ps1 v$ScriptVersion"
Write-Log "INFO" "BasePath=$BasePath"
Write-Log "INFO" "SitesPath=$SitesPath"
Write-Log "INFO" "RulesPath=$RulesPath"
Write-Log "INFO" "DataPath=$DataPath"
Write-Log "INFO" "VersionDir=$VersionDir"

# ----------------------[ Default Whitelist ]----------------------------------
# Relative paths under /sites. EXACTLY your approved list.
$DefaultWhitelist = @(
    "abc.net.au\abc.net.au_syd.channels.xml",
    "directv.com\directv.com.channels.xml",

    "epgshare01.online\epgshare01.online_AU1.channels.xml",
    "epgshare01.online\epgshare01.online_CA1.channels.xml",
    "epgshare01.online\epgshare01.online_UK1.channels.xml",
    "epgshare01.online\epgshare01.online_US1.channels.xml",
    "epgshare01.online\epgshare01.online_US_LOCALS2.channels.xml",

    "player.ee.co.uk\player.ee.co.uk.channels.xml",
    "plex.tv\plex.tv.channels.xml",

    "pluto.tv\pluto.tv_us.channels.xml",
    "pluto.tv\pluto.tv_ca.channels.xml",
    "pluto.tv\pluto.tv_uk.channels.xml",

    "tv24.co.uk\tv24.co.uk.channels.xml",
    "tvguide.com\tvguide.com.channels.xml",
    "tvinsider.com\tvinsider.com.channels.xml",
    "virgintvgo.virginmedia.com\virgintvgo.virginmedia.com.channels.xml",

    "freeview.co.uk\freeview.co.uk.channels.xml"   # you explicitly approved this in whitelist
)

# ----------------------[ Rules CSV Bootstrapping ]----------------------------
if (!(Test-Path $SourcesCsv)) {
    Write-Log "OK" "Creating rules whitelist file: $SourcesCsv"
    "relative_path,enabled,note" | Out-File -FilePath $SourcesCsv -Encoding UTF8
    foreach ($rp in $DefaultWhitelist) {
        "{0},true,approved_whitelist" -f $rp | Add-Content -Path $SourcesCsv -Encoding UTF8
    }
}

# ----------------------[ Load Whitelist ]-------------------------------------
$Whitelist = @()
try {
    $srcRows = Import-Csv $SourcesCsv
    foreach ($r in $srcRows) {
        if ($r.enabled -match "^(true|1|yes)$") {
            $Whitelist += $r.relative_path
        }
    }
} catch {
    Write-Log "WARN" "Could not read $SourcesCsv, using internal defaults. Error=$($_.Exception.Message)"
    $Whitelist = $DefaultWhitelist
}

if ($Whitelist.Count -eq 0) {
    Write-Log "WARN" "Whitelist empty. Falling back to internal defaults."
    $Whitelist = $DefaultWhitelist
}

Write-Log "INFO" ("Whitelist files enabled={0}" -f $Whitelist.Count)

# ----------------------[ Helper: country infer ]------------------------------
function Infer-Country {
    param([string]$RelativePath, [string]$XmlTvId)

    $file = [System.IO.Path]::GetFileName($RelativePath).ToLowerInvariant()

    if ($file -match "_au" -or $file -match "pluto\.tv_au" -or $XmlTvId -match "\.au@") { return "AU" }
    if ($file -match "_ca" -or $file -match "pluto\.tv_ca" -or $XmlTvId -match "\.ca@") { return "CA" }
    if ($file -match "_uk" -or $file -match "freeview\.co\.uk" -or $file -match "tv24\.co\.uk" -or $XmlTvId -match "\.uk@") { return "UK" }
    if ($file -match "_us" -or $file -match "directv\.com" -or $file -match "pluto\.tv_us" -or $XmlTvId -match "\.us@") { return "US" }

    return ""
}

# ----------------------[ Helper: hd infer ]-----------------------------------
function Infer-HDFlag {
    param([string]$Name, [string]$XmlTvId)

    $s = ($Name + " " + $XmlTvId).ToUpperInvariant()
    if ($s -match "\bHD\b" -or $s -match "\bUHD\b" -or $s -match "1080" -or $s -match "4K") { return "HD" }
    return "SD"
}

# ----------------------[ Helper: name extract ]-------------------------------
function Extract-ChannelName {
    param([xml]$Doc, $Node)

    # 1) InnerText (most common)
    $textName = ($Node.InnerText ?? "").Trim()
    if ($textName) { return $textName }

    # 2) @name attribute (some sources)
    $attrName = ($Node.GetAttribute("name") ?? "").Trim()
    if ($attrName) { return $attrName }

    # 3) <display-name> child (xmltv-ish variants)
    $dn = $Node.SelectSingleNode("display-name")
    if ($dn -and ($dn.InnerText.Trim())) { return $dn.InnerText.Trim() }

    return ""
}

# ---------------------------[ Collect Rows ]----------------------------------
$AllRows = New-Object System.Collections.Generic.List[object]

foreach ($rel in $Whitelist) {
    $full = Join-Path $SitesPath $rel

    if (!(Test-Path $full)) {
        Write-Log "WARN" "Missing whitelist file: $rel (skipping)"
        continue
    }

    Write-Log "INFO" "Loading: $rel"

    try {
        [xml]$doc = Get-Content -LiteralPath $full -Raw -Encoding UTF8
        $channels = $doc.SelectNodes("//channel")

        if (-not $channels) {
            Write-Log "WARN" "No <channel> nodes found in $rel"
            continue
        }

        foreach ($ch in $channels) {
            $site    = ($ch.GetAttribute("site")    ?? "").Trim()
            $site_id = ($ch.GetAttribute("site_id") ?? "").Trim()
            $lang    = ($ch.GetAttribute("lang")    ?? "").Trim()
            $xmltv   = ($ch.GetAttribute("xmltv_id")?? "").Trim()
            $c_attr  = ($ch.GetAttribute("country") ?? "").Trim()

            $name = Extract-ChannelName -Doc $doc -Node $ch
            if (-not $name) { $name = "" }

            $country = if ($c_attr) { $c_attr.ToUpperInvariant() } else { (Infer-Country -RelativePath $rel -XmlTvId $xmltv) }
            $hd_flag = Infer-HDFlag -Name $name -XmlTvId $xmltv

            $row = [PSCustomObject]@{
                site     = $site
                relative = $rel
                name     = $name
                xmltv_id = $xmltv
                lang     = $lang
                site_id  = $site_id
                country  = $country
                hd_flag  = $hd_flag
            }

            $AllRows.Add($row) | Out-Null
        }
    } catch {
        Write-Log "ERROR" "Failed parsing $rel : $($_.Exception.Message)"
        continue
    }
}

$rawCount = $AllRows.Count
Write-Log "INFO" "Extraction complete. Total rows collected (raw): $rawCount"

# ---------------------------[ Deduce Duplicates ]-----------------------------
# Remove exact duplicates using composite key
$seen = @{}
$deduped = New-Object System.Collections.Generic.List[object]

foreach ($r in $AllRows) {
    $key = "{0}|{1}|{2}|{3}|{4}|{5}" -f $r.site, $r.relative, $r.site_id, $r.xmltv_id, $r.name, $r.lang
    if (-not $seen.ContainsKey($key)) {
        $seen[$key] = $true
        $deduped.Add($r) | Out-Null
    }
}

$dedupCount = $deduped.Count
$dupsRemoved = $rawCount - $dedupCount
Write-Log "INFO" "Deduped rows=$dedupCount (duplicates removed=$dupsRemoved)"

# ---------------------------[ Write Outputs ]---------------------------------
try {
    $deduped | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $MasterCsv
    Write-Log "OK" "Wrote master CSV: $MasterCsv"

    $deduped | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $VersionCsv
    Write-Log "OK" "Wrote versioned dataset: $VersionCsv"
} catch {
    Write-Log "ERROR" "Failed writing outputs: $($_.Exception.Message)"
    throw
}

Write-Log "INFO" "Step 1 complete."
Write-Log "INFO" "Finished step1_extract_all_sites_master_channels.ps1"
