<# 
Script Name: step0_setup_pipeline.ps1
Purpose: Step 0 of the AJPs Custom EPG pipeline. Ensures folder structure exists,
         creates rules scaffolding + versioned folders + baseline templates.
Author: ChatGPT for Andrew Pearen
Created: 2025-11-23
Last Updated: 2025-11-23
Version: 2.0

Run:
PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step0_setup_pipeline.ps1

Notes:
- Avoids Start-Transcript to prevent file locking.
- All logging goes to custom\logs\step0_setup_pipeline.log
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --------------------------- Paths ---------------------------
$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$CustomPath = Join-Path $BasePath "custom"

$Folders = @(
    "archive",
    "baseline",
    "cache",
    "data",
    "enrich",
    "filter",
    "grabs",
    "gui",
    "logs",
    "match",
    "merge",
    "output",
    "rules",
    "scripts"
)

$LogPath = Join-Path $CustomPath "logs"
$LogFile = Join-Path $LogPath "step0_setup_pipeline.log"

# --------------------------- Logging ---------------------------
function Write-Log {
    param(
        [Parameter(Mandatory=$true)][string]$Level,
        [Parameter(Mandatory=$true)][string]$Message
    )
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$ts][$Level] $Message"

    # Console
    switch ($Level) {
        "ERROR" { Write-Host $line -ForegroundColor Red }
        "WARN"  { Write-Host $line -ForegroundColor Yellow }
        "OK"    { Write-Host $line -ForegroundColor Green }
        default { Write-Host $line }
    }

    # File (with retry in case of brief lock)
    $retries = 5
    for ($i=0; $i -lt $retries; $i++) {
        try {
            Add-Content -Path $LogFile -Value $line
            break
        } catch {
            Start-Sleep -Milliseconds 150
            if ($i -eq ($retries-1)) { throw }
        }
    }
}

# --------------------------- Main ---------------------------
try {
    if (!(Test-Path $LogPath)) {
        New-Item -ItemType Directory -Force -Path $LogPath | Out-Null
    }

    Write-Log "INFO" "Starting step0_setup_pipeline.ps1 v2.0"
    Write-Log "INFO" "BasePath=$BasePath"
    Write-Log "INFO" "CustomPath=$CustomPath"

    foreach ($f in $Folders) {
        $p = Join-Path $CustomPath $f
        if (!(Test-Path $p)) {
            New-Item -ItemType Directory -Force -Path $p | Out-Null
            Write-Log "OK" "Created folder: $p"
        } else {
            Write-Log "DEBUG" "Folder exists: $p"
        }
    }

    # Versioned subfolders
    $VersionedTargets = @(
        (Join-Path $CustomPath "data\versioned-master"),
        (Join-Path $CustomPath "baseline\versioned"),
        (Join-Path $CustomPath "rules\versioned")
    )

    foreach ($vt in $VersionedTargets) {
        if (!(Test-Path $vt)) {
            New-Item -ItemType Directory -Force -Path $vt | Out-Null
            Write-Log "OK" "Created versioned folder: $vt"
        }
    }

    # ---------------- Rules Templates ----------------
    $RulesPath = Join-Path $CustomPath "rules"

    $Step2Rules = Join-Path $RulesPath "step2_select_core_rules.csv"
    if (!(Test-Path $Step2Rules)) {
@"
rule_id,action,field,regex,countries,notes
R001,DROP,name,"(?i)\b(church|god|faith|jesus|islam|quran|tlm|daystar|trinity|worship|miracle)\b",,religious reject
R002,DROP,name,"(?i)\b(kids?|jr\.?|junior|cartoon|nick|nickelodeon|disney jr|pbs kids|treehouse|youth)\b",,kids reject
R003,DROP,name,"(?i)\b(sports?|espn|tsn|sport|sky sports|fox sports|golf|nba|nfl|mlb|nhl|f1|ufc)\b",,sports reject
R004,DROP,name,"(?i)\b(radio|fm|am\s?\d+|music only|audio)\b",,radio reject
R005,DROP,name,"(?i)\b(news)\b",,news reject base (allowlist applies later)
"@ | Set-Content -Path $Step2Rules -Encoding UTF8
        Write-Log "OK" "Created template: $Step2Rules"
    }

    $OverridesKeep = Join-Path $RulesPath "overrides_keep.csv"
    if (!(Test-Path $OverridesKeep)) {
@"
site,relative,name,xmltv_id,lang,site_id,country,hd_flag,comment
"@ | Set-Content -Path $OverridesKeep -Encoding UTF8
        Write-Log "OK" "Created template: $OverridesKeep"
    }

    $OverridesDrop = Join-Path $RulesPath "overrides_drop.csv"
    if (!(Test-Path $OverridesDrop)) {
@"
site,relative,name,xmltv_id,lang,site_id,country,hd_flag,comment
"@ | Set-Content -Path $OverridesDrop -Encoding UTF8
        Write-Log "OK" "Created template: $OverridesDrop"
    }

    # Step-specific rules placeholders
    $StepRules = @(
        "step3_canada_specialty_rules.csv",
        "step4_us_cable_rules.csv",
        "step5_uk_primary_rules.csv",
        "step6_au_primary_rules.csv"
    )

    foreach ($sr in $StepRules) {
        $rp = Join-Path $RulesPath $sr
        if (!(Test-Path $rp)) {
@"
rule_id,action,field,regex,countries,notes
"@ | Set-Content -Path $rp -Encoding UTF8
            Write-Log "OK" "Created template: $rp"
        }
    }

    Write-Log "OK" "Step 0 complete. Folder + rules scaffolding ready."
}
catch {
    Write-Log "ERROR" ("Step 0 failed: " + $_.Exception.Message)
    throw
}
finally {
    Write-Log "INFO" "Finished step0_setup_pipeline.ps1"
}
