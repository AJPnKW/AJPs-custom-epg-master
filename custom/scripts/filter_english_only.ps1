<#
filter_english_only.ps1 — Step 2
Purpose: Filter master channels for AU/UK/US/CA English only, remove sports/kids/religious/radio,
         keep limited news allowlist, prefer SD if HD duplicates exist, apply overrides_keep.csv.
Version: 2.2
Author: ChatGPT for Andrew
Run:  C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\filter_english_only.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --------------------------[ paths ]--------------------------
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$BasePath   = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
$CustomPath = Join-Path $BasePath "custom"

$InputCSV   = Join-Path $CustomPath "data\all_sites_master_channels.csv"
$FilterPath = Join-Path $CustomPath "filter"
$VersionPath= Join-Path $FilterPath "versioned"
$OverridesKeepPath = Join-Path $FilterPath "overrides_keep.csv"

$LogPath    = Join-Path $CustomPath "logs"
$LogFileBase    = Join-Path $LogPath "filter_english_only.log"
$TranscriptFile = Join-Path $LogPath "filter_english_only.transcript.log"
$LogFile        = $LogFileBase

$OutKeep    = Join-Path $FilterPath "ajps_step2_filtered_channels.csv"
$OutDrop    = Join-Path $FilterPath "ajps_step2_filtered_out.csv"

# --------------------------[ helper core ]--------------------------
function Get-Stamp { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }
function Get-FileStamp { (Get-Date).ToString("yyyyMMdd_HHmmss") }

function Write-HostSafe { param([string]$Text) Microsoft.PowerShell.Utility\Write-Host $Text }

function Write-Log {
    param([string]$Level,[string]$Message)

    $line = "[{0}][{1}] {2}" -f (Get-Stamp), $Level.ToUpper(), $Message
    Write-HostSafe $line

    $maxTries = 5
    for ($i=1; $i -le $maxTries; $i++) {
        try { Add-Content -Path $LogFile -Value $line; return }
        catch {
            Start-Sleep -Milliseconds 200
            if ($i -eq $maxTries) {
                $fallback = Join-Path $LogPath ("filter_english_only.{0}.log" -f (Get-FileStamp))
                $LogFile = $fallback
                try { Add-Content -Path $LogFile -Value $line; Write-HostSafe "[WARN] Log locked. Fallback log in use." }
                catch { Write-HostSafe "[ERROR] Could not write logs." }
            }
        }
    }
}

function Ensure-Folder { param([string]$Path) if(!(Test-Path $Path)){ New-Item -ItemType Directory -Path $Path -Force | Out-Null } }

function Normalize-Name {
    param([string]$Name)
    if (-not $Name) { return "" }
    ($Name.ToLower() -replace '[^\p{L}\p{Nd}]','' -replace '\s+','').Trim()
}

# --------------------------[ reject/allow rules ]--------------------------
$CountriesInScope = @("AU","UK","US","CA")

$SportsReject = '(?i)\b(sports?|espn|tsn|fox sports|beins?|nba|nfl|nhl|mlb|golf|tennis|cricket|formula|f1|ufc|wwe|boxing|racing)\b'
$KidsReject   = '(?i)\b(kids?|jr\.?|junior|cartoon|disney jr|nick jr|nickelodeon|boomerang|pbs kids|cbeebies)\b'
$RelReject    = '(?i)\b(christian|faith|god|jesus|church|islam|muslim|quran|mormon|tbn|daystar|eWTN|hope channel)\b'
$RadioReject  = '(?i)\b(radio|fm\b|am\b|audio only)\b'

# news allowlist (only these survive)
$NewsAllow = @(
    "cnn","msnbc","bbcnews","bbc news","ctv news","ctv newschannel","cbc news","cbc news toronto","citytv news","citytv news toronto"
)

$NewsReject = '(?i)\b(news|newschannel|headline|breaking news)\b'

# --------------------------[ load overrides_keep.csv ]--------------------------
$OverridesKeep = @()
if (Test-Path $OverridesKeepPath) {
    try { $OverridesKeep = Import-Csv -LiteralPath $OverridesKeepPath }
    catch { Write-Log "WARN" "Could not read overrides_keep.csv. Continuing without overrides." }
}

# build fast override keys
$OverrideKeys = New-Object System.Collections.Generic.HashSet[string]
foreach ($o in $OverridesKeep) {
    $key = ""
    if ($o.site -and $o.site_id) { $key = ("{0}|{1}" -f $o.site.ToLower(), $o.site_id) }
    elseif ($o.xmltv_id)         { $key = ("xmltv|{0}" -f $o.xmltv_id.ToLower()) }
    elseif ($o.name -and $o.country) { $key = ("name|{0}|{1}" -f (Normalize-Name $o.name), $o.country.ToUpper()) }
    if ($key) { $OverrideKeys.Add($key) | Out-Null }
}

function Is-OverrideKeep {
    param($r)
    if ($r.site -and $r.site_id) {
        $k = ("{0}|{1}" -f $r.site.ToLower(), $r.site_id)
        if ($OverrideKeys.Contains($k)) { return $true }
    }
    if ($r.xmltv_id) {
        $k = ("xmltv|{0}" -f $r.xmltv_id.ToLower())
        if ($OverrideKeys.Contains($k)) { return $true }
    }
    if ($r.name -and $r.country) {
        $k = ("name|{0}|{1}" -f (Normalize-Name $r.name), $r.country.ToUpper())
        if ($OverrideKeys.Contains($k)) { return $true }
    }
    return $false
}

# --------------------------[ main ]--------------------------
Ensure-Folder $LogPath
Ensure-Folder $FilterPath
Ensure-Folder $VersionPath

$TranscriptStarted = $false

try {
    Start-Transcript -Path $TranscriptFile -Append | Out-Null
    $TranscriptStarted = $true

    Write-Log "INFO" "Starting filter_english_only.ps1 (Step 2 v2.2)"
    Write-Log "INFO" "InputCSV=$InputCSV"
    Write-Log "INFO" "FilterPath=$FilterPath"
    Write-Log "INFO" "VersionPath=$VersionPath"

    if(!(Test-Path $InputCSV)){ throw "Missing input master CSV: $InputCSV" }

    $raw = Import-Csv -LiteralPath $InputCSV
    Write-Log "INFO" ("Loaded raw rows={0}" -f $raw.Count)

    # --------- dedupe identical rows ---------
    $seen = New-Object System.Collections.Generic.HashSet[string]
    $deduped = New-Object System.Collections.Generic.List[object]
    $dupCount = 0

    foreach ($r in $raw) {
        $key = ("{0}|{1}|{2}|{3}" -f
            ($r.site ?? "").ToLower(),
            ($r.site_id ?? ""),
            ($r.xmltv_id ?? "").ToLower(),
            (Normalize-Name $r.name)
        )
        if ($seen.Contains($key)) { $dupCount++; continue }
        $seen.Add($key) | Out-Null
        $deduped.Add($r)
    }

    Write-Log "INFO" ("Deduped rows={0} (duplicates removed={1})" -f $deduped.Count, $dupCount)

    $kept = New-Object System.Collections.Generic.List[object]
    $dropped = New-Object System.Collections.Generic.List[object]
    $dropStats = @{}

    foreach ($r in $deduped) {

        $name = ($r.name ?? "").Trim()
        $norm = (Normalize-Name $name)
        $country = (($r.country ?? "") -replace '\s+','').ToUpper()
        $lang = (($r.lang ?? "")).ToLower()
        $hd = (($r.hd_flag ?? "SD")).ToUpper()

        # override keep wins immediately
        if (Is-OverrideKeep $r) {
            $r | Add-Member -NotePropertyName reason -NotePropertyValue "override_keep" -Force
            $kept.Add($r)
            continue
        }

        # country scope
        if ($country -and ($CountriesInScope -notcontains $country)) {
            $r | Add-Member reason "country_out_of_scope" -Force
            $dropped.Add($r)
            $dropStats["country_out_of_scope"] = ($dropStats["country_out_of_scope"] ?? 0) + 1
            continue
        }

        # lang scope (keep blank or en)
        if ($lang -and $lang -ne "en") {
            $r | Add-Member reason "non_english_lang" -Force
            $dropped.Add($r)
            $dropStats["non_english_lang"] = ($dropStats["non_english_lang"] ?? 0) + 1
            continue
        }

        # religious / radio / sports / kids
        if ($name -match $RelReject) {
            $r | Add-Member reason "religious_channel" -Force
            $dropped.Add($r)
            $dropStats["religious_channel"] = ($dropStats["religious_channel"] ?? 0) + 1
            continue
        }
        if ($name -match $RadioReject) {
            $r | Add-Member reason "radio_channel" -Force
            $dropped.Add($r)
            $dropStats["radio_channel"] = ($dropStats["radio_channel"] ?? 0) + 1
            continue
        }
        if ($name -match $SportsReject) {
            $r | Add-Member reason "sports_channel" -Force
            $dropped.Add($r)
            $dropStats["sports_channel"] = ($dropStats["sports_channel"] ?? 0) + 1
            continue
        }
        if ($name -match $KidsReject) {
            $r | Add-Member reason "kids_channel" -Force
            $dropped.Add($r)
            $dropStats["kids_channel"] = ($dropStats["kids_channel"] ?? 0) + 1
            continue
        }

        # news filtering
        if ($name -match $NewsReject) {
            $allowed = $false
            foreach ($a in $NewsAllow) {
                if ($norm -like "*$(Normalize-Name $a)*") { $allowed = $true; break }
            }
            if (-not $allowed) {
                $r | Add-Member reason "news_channel_not_allowed" -Force
                $dropped.Add($r)
                $dropStats["news_channel_not_allowed"] = ($dropStats["news_channel_not_allowed"] ?? 0) + 1
                continue
            }
        }

        # passed all rules
        $r | Add-Member reason "kept" -Force
        $kept.Add($r)
    }

    Write-Log "INFO" ("Good rows kept={0}" -f $kept.Count)
    Write-Log "INFO" ("Bad rows filtered={0}" -f $dropped.Count)

    # export outputs
    $kept | Select-Object site,relative,name,xmltv_id,lang,site_id,country,hd_flag,reason |
        Export-Csv -LiteralPath $OutKeep -NoTypeInformation -Encoding UTF8

    $dropped | Select-Object site,relative,name,xmltv_id,lang,site_id,country,hd_flag,reason |
        Export-Csv -LiteralPath $OutDrop -NoTypeInformation -Encoding UTF8

    # versioned copies
    $stamp = Get-FileStamp
    Copy-Item $OutKeep (Join-Path $VersionPath "ajps_step2_filtered_channels_$stamp.csv") -Force
    Copy-Item $OutDrop (Join-Path $VersionPath "ajps_step2_filtered_out_$stamp.csv") -Force

    Write-Log "INFO" "Wrote filtered CSV: $OutKeep"
    Write-Log "INFO" "Wrote filtered-out audit CSV: $OutDrop"
    Write-Log "INFO" "Versioned outputs written for stamp=$stamp"

    Write-Log "INFO" "Drops by rule:"
    foreach ($k in $dropStats.Keys) {
        Write-Log "INFO" ("  {0}={1}" -f $k, $dropStats[$k])
    }

    Write-Log "INFO" "Done."
}
catch {
    Write-Log "ERROR" $_.Exception.Message
    throw
}
finally {
    if ($TranscriptStarted) { try { Stop-Transcript | Out-Null } catch {} }
}
