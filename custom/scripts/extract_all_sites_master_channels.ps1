<# 
Script Name: extract_all_sites_master_channels.ps1
Purpose: Step 1. Extract channel nodes from selected local site XML files into
         a master CSV dataset for downstream steps.
Author: ChatGPT for Andrew Pearen
Created: 2025-11-23
Last Updated: 2025-11-23
Version: 2.1

Run:
PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\extract_all_sites_master_channels.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --------------------------- Paths ---------------------------
$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$SitesPath  = Join-Path $BasePath "sites"
$CustomPath = Join-Path $BasePath "custom"
$DataPath   = Join-Path $CustomPath "data"
$LogPath    = Join-Path $CustomPath "logs"

$OutMaster  = Join-Path $DataPath "all_sites_master_channels.csv"
$VersionDir = Join-Path $DataPath "versioned-master"
$Stamp      = (Get-Date).ToString("yyyyMMdd_HHmmss")
$OutVersion = Join-Path $VersionDir "master_channels_$Stamp.csv"

$LogFile = Join-Path $LogPath "extract_all_sites_master_channels.log"

# --------------------------- Logging ---------------------------
function Write-Log {
    param([string]$Level, [string]$Message)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$ts][$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

# --------------------------- Site scope ---------------------------
$ScopedFiles = @(
    "abc.net.au\abc.net.au_syd.channels.xml",
    "directv.com\directv.com.channels.xml",
    "epgshare01.online\epgshare01.online_AU1.channels.xml",
    "epgshare01.online\epgshare01.online_CA1.channels.xml",
    "epgshare01.online\epgshare01.online_UK1.channels.xml",
    "epgshare01.online\epgshare01.online_US_LOCALS2.channels.xml",
    "epgshare01.online\epgshare01.online_US1.channels.xml",
    "player.ee.co.uk\player.ee.co.uk.channels.xml",
    "plex.tv\plex.tv.channels.xml",
    "pluto.tv\pluto.tv_us.channels.xml",
    "pluto.tv\pluto.tv_ca.channels.xml",
    "pluto.tv\pluto.tv_uk.channels.xml",
    "freeview.co.uk\freeview.co.uk.channels.xml",
    "tv24.co.uk\tv24.co.uk.channels.xml",
    "tvguide.com\tvguide.com.channels.xml",
    "tvinsider.com\tvinsider.com.channels.xml",
    "virgintvgo.virginmedia.com\virgintvgo.virginmedia.com.channels.xml"
)

# --------------------------- Helpers ---------------------------
function Infer-Country {
    param([string]$XmltvId, [string]$Site, [string]$Relative)
    if ($XmltvId -match "\.(us|uk|ca|au)@") { return $matches[1].ToUpper() }
    switch -Regex ($Site) {
        "abc\.net\.au|epgshare01\.online" { if ($Relative -match "_AU1") { return "AU" } }
        "freeview\.co\.uk|tv24\.co\.uk|virginmedia" { return "UK" }
        "directv\.com|tvguide\.com|tvinsider\.com" { return "US" }
        "pluto\.tv" {
            if ($Relative -match "_ca") { return "CA" }
            if ($Relative -match "_uk") { return "UK" }
            if ($Relative -match "_us") { return "US" }
        }
    }
    return ""
}

function Infer-HdFlag {
    param([string]$Name)
    if ($Name -match "(?i)\b(HD|UHD|4K|1080|720)\b") { return "HD" }
    return "SD"
}

# --------------------------- Main ---------------------------
try {
    if (!(Test-Path $LogPath)) { New-Item -ItemType Directory -Force -Path $LogPath | Out-Null }
    if (!(Test-Path $VersionDir)) { New-Item -ItemType Directory -Force -Path $VersionDir | Out-Null }

    Write-Log "INFO" "Starting extract_all_sites_master_channels.ps1 v2.1"
    Write-Log "INFO" "BasePath=$BasePath"
    Write-Log "INFO" "SitesPath=$SitesPath"

    $SiteFiles = foreach ($rel in $ScopedFiles) {
        $full = Join-Path $SitesPath $rel
        if (Test-Path $full) {
            [pscustomobject]@{ relative = $rel; full = $full; site = ($rel.Split("\")[0]) }
        } else {
            Write-Log "WARN" "Missing scoped file: $rel"
        }
    }

    Write-Log "INFO" ("Total XML source files: " + $SiteFiles.Count)

    $AllRows = [System.Collections.Concurrent.ConcurrentBag[object]]::new()

    $SiteFiles | ForEach-Object -Parallel {
        param($sf)

        $rel  = $sf.relative
        $full = $sf.full
        $site = $sf.site

        try {
            $doc = [xml](Get-Content -Raw -Path $full)

            # Support both <tv><channel> and root-level <channel>
            $channels = @()
            if ($doc.tv -and $doc.tv.channel) { $channels = $doc.tv.channel }
            elseif ($doc.channel) { $channels = $doc.channel }

            foreach ($ch in $channels) {
                $name = ($ch.InnerText ?? "").Trim()
                if ([string]::IsNullOrWhiteSpace($name)) { $name = "" }

                $xmltv = ($ch.xmltv_id ?? "").Trim()
                $lang  = ($ch.lang ?? "").Trim()
                $sid   = ($ch.site_id ?? "").Trim()
                $cny   = Infer-Country -XmltvId $xmltv -Site $site -Relative $rel
                $hd    = Infer-HdFlag -Name $name

                $row = [pscustomobject]@{
                    site     = $site
                    relative = $rel
                    name     = $name
                    xmltv_id = $xmltv
                    lang     = $lang
                    site_id  = $sid
                    country  = $cny
                    hd_flag  = $hd
                }

                $using:AllRows.Add($row)
            }
        }
        catch {
            $msg = $_.Exception.Message
            Add-Content -Path $using:LogFile -Value ("[ERROR] Parse failed for $rel : $msg")
        }

    } -ThrottleLimit 6

    $Rows = $AllRows.ToArray()

    Write-Log "INFO" ("Extraction complete. Total rows collected: " + $Rows.Count)

    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutMaster
    Write-Log "INFO" "Wrote master CSV: $OutMaster"

    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutVersion
    Write-Log "INFO" "Wrote versioned dataset: $OutVersion"

    Write-Log "OK" "Step 1 complete."
}
catch {
    Write-Log "ERROR" ("Step 1 failed: " + $_.Exception.Message)
    throw
}
finally {
    Write-Log "INFO" "Finished extract_all_sites_master_channels.ps1"
}
