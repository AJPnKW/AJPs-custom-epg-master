<#
-------------------------------------------------------------------------------
Script Name: step2_select_core_channels.ps1
Purpose    : STEP 2 of AJP Custom EPG pipeline.
             POSITIVE extraction ("keep") from Step1 master list using:
               - Rules CSV (editable)
               - Overrides keep CSV (manual force-keep list)

Author     : ChatGPT for Andrew Pearen
Version    : 2.0
Created    : 2025-11-23
Updated    : 2025-11-23

Run Cmd    :
  PowerShell:
  C:\Users\<user>\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step2_select_core_channels.ps1

Inputs     :
  1) Master channels CSV from Step 1:
     custom\data\all_sites_master_channels.csv

  2) Rules file (created in Step 0; edit anytime):
     custom\rules\step2_select_core_rules.csv

  3) Overrides keep file (manual safe list; optional):
     custom\rules\overrides_keep.csv
     If missing/empty ⇒ ignored.

Outputs    :
  - Kept channels:
      custom\filter\ajps_step2_kept_channels.csv
  - Remaining channels (not selected yet):
      custom\filter\ajps_step2_remaining.csv
  - Versioned copies:
      custom\filter\versioned\step2_kept_yyyyMMdd_HHmmss.csv
      custom\filter\versioned\step2_remaining_yyyyMMdd_HHmmss.csv
  - Audit drops/keeps:
      custom\filter\ajps_step2_audit.csv
  - Log:
      custom\logs\step2_select_core_channels.log

Notes:
  - This is a "keep" step. Rules with action=keep add rows.
    Rules with action=drop remove rows EVEN if a keep rule matched,
    unless the row is in overrides_keep.
  - Overrides_keep ALWAYS wins.
  - Duplicate removal is exact-row dedupe before rules.
-------------------------------------------------------------------------------
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------[ Paths & Setup ]---------------------------------
$ScriptVersion = "2.0"
$Stamp         = Get-Date -Format "yyyyMMdd_HHmmss"

$ScriptRoot = Split-Path -Parent $PSCommandPath
$CustomPath = Split-Path -Parent $ScriptRoot
$BasePath   = Split-Path -Parent $CustomPath

$DataPath   = Join-Path $CustomPath "data"
$RulesPath  = Join-Path $CustomPath "rules"
$FilterPath = Join-Path $CustomPath "filter"
$LogsPath   = Join-Path $CustomPath "logs"
$VersionDir = Join-Path $FilterPath "versioned"

$InputCsv   = Join-Path $DataPath "all_sites_master_channels.csv"
$RulesCsv   = Join-Path $RulesPath "step2_select_core_rules.csv"
$OverrideCsv= Join-Path $RulesPath "overrides_keep.csv"

$KeptCsv    = Join-Path $FilterPath "ajps_step2_kept_channels.csv"
$RemainCsv  = Join-Path $FilterPath "ajps_step2_remaining.csv"
$AuditCsv   = Join-Path $FilterPath "ajps_step2_audit.csv"

$KeptVer    = Join-Path $VersionDir ("step2_kept_{0}.csv" -f $Stamp)
$RemainVer  = Join-Path $VersionDir ("step2_remaining_{0}.csv" -f $Stamp)
$AuditVer   = Join-Path $VersionDir ("step2_audit_{0}.csv" -f $Stamp)

$LogFile    = Join-Path $LogsPath "step2_select_core_channels.log"

foreach ($d in @($FilterPath,$LogsPath,$VersionDir)) {
    if (!(Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

# ---------------------------[ Logging ]---------------------------------------
function Write-Log {
    param(
        [Parameter(Mandatory=$true)][ValidateSet("INFO","OK","WARN","ERROR","DEBUG")]$Level,
        [Parameter(Mandatory=$true)][string]$Message
    )
    $ts   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level, $Message

    if ($Level -ne "DEBUG") { Write-Host $line }

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

Write-Log "INFO" "Starting step2_select_core_channels.ps1 v$ScriptVersion"
Write-Log "INFO" "InputCsv=$InputCsv"
Write-Log "INFO" "RulesCsv=$RulesCsv"
Write-Log "INFO" "OverrideCsv=$OverrideCsv"
Write-Log "INFO" "FilterPath=$FilterPath"
Write-Log "INFO" "VersionDir=$VersionDir"

# ---------------------------[ Guards ]----------------------------------------
if (!(Test-Path $InputCsv)) {
    Write-Log "ERROR" "Missing input file: $InputCsv"
    throw "Input CSV not found."
}
if (!(Test-Path $RulesCsv)) {
    Write-Log "ERROR" "Missing rules file: $RulesCsv"
    throw "Rules CSV not found."
}

# ---------------------------[ Load Master ]-----------------------------------
$rowsRaw = Import-Csv $InputCsv
Write-Log "INFO" ("Loaded raw rows={0}" -f $rowsRaw.Count)

# ----------------------[ Dedupe exact duplicates ]----------------------------
$seen = @{}
$rows = New-Object System.Collections.Generic.List[object]

foreach ($r in $rowsRaw) {
    $key = "{0}|{1}|{2}|{3}|{4}|{5}|{6}" -f $r.site, $r.relative, $r.site_id, $r.xmltv_id, $r.name, $r.lang, $r.country
    if (-not $seen.ContainsKey($key)) {
        $seen[$key] = $true
        $rows.Add($r) | Out-Null
    }
}

$dupsRemoved = $rowsRaw.Count - $rows.Count
Write-Log "INFO" ("Deduped rows={0} (duplicates removed={1})" -f $rows.Count, $dupsRemoved)

# ---------------------------[ Load Overrides ]--------------------------------
$overrideList = @()
if (Test-Path $OverrideCsv) {
    try {
        $overrideList = Import-Csv $OverrideCsv
        Write-Log "INFO" ("Overrides loaded={0}" -f $overrideList.Count)
    } catch {
        Write-Log "WARN" "Overrides file unreadable. Ignoring. Error=$($_.Exception.Message)"
        $overrideList = @()
    }
} else {
    Write-Log "INFO" "Overrides file not found. Skipping."
}

# Build fast lookup sets from overrides
# Priority match: site+xmltv_id, else site+site_id, else site+name
$ovXmltv = @{}
$ovSiteId= @{}
$ovName  = @{}

foreach ($o in $overrideList) {
    $s  = ($o.site ?? "").Trim()
    $xi = ($o.xmltv_id ?? "").Trim()
    $si = ($o.site_id ?? "").Trim()
    $nm = ($o.name ?? "").Trim()

    if ($s -and $xi) { $ovXmltv["$s|$xi"] = $true; continue }
    if ($s -and $si) { $ovSiteId["$s|$si"] = $true; continue }
    if ($s -and $nm) { $ovName["$s|$nm"]   = $true; continue }
}

function Is-OverrideKeep {
    param($r)
    $s  = ($r.site ?? "").Trim()
    $xi = ($r.xmltv_id ?? "").Trim()
    $si = ($r.site_id ?? "").Trim()
    $nm = ($r.name ?? "").Trim()

    if ($s -and $xi -and $ovXmltv.ContainsKey("$s|$xi")) { return $true }
    if ($s -and $si -and $ovSiteId.ContainsKey("$s|$si")) { return $true }
    if ($s -and $nm -and $ovName.ContainsKey("$s|$nm")) { return $true }
    return $false
}

# ---------------------------[ Load Rules ]------------------------------------
$rulesRaw = Import-Csv $RulesCsv
$rules = @()

foreach ($rw in $rulesRaw) {
    if (($rw.enabled ?? "") -match "^(true|1|yes)$") {
        $rules += $rw
    }
}
Write-Log "INFO" ("Rules enabled={0}" -f $rules.Count)

# ----------------------[ Rule Evaluation Helpers ]----------------------------
function Get-FieldValue {
    param($row, [string]$field)

    switch ($field.ToLowerInvariant()) {
        "name"     { return ($row.name ?? "") }
        "site"     { return ($row.site ?? "") }
        "country"  { return ($row.country ?? "") }
        "lang"     { return ($row.lang ?? "") }
        "xmltv_id" { return ($row.xmltv_id ?? "") }
        "site_id"  { return ($row.site_id ?? "") }
        default    { return "" }
    }
}

function Match-Rule {
    param($row, $rule)

    $field   = ($rule.field ?? "").Trim()
    $pattern = ($rule.pattern ?? "").Trim()
    $flags   = ($rule.flags ?? "").Trim()

    if (-not $field -or -not $pattern) { return $false }

    $val = (Get-FieldValue -row $row -field $field)
    if (-not $val) { $val = "" }

    # Flags: i = ignore case
    $opts = [System.Text.RegularExpressions.RegexOptions]::None
    if ($flags -match "i") { $opts = $opts -bor [System.Text.RegularExpressions.RegexOptions]::IgnoreCase }

    try {
        return [regex]::IsMatch($val, $pattern, $opts)
    } catch {
        Write-Log "WARN" ("Bad regex in rule_id={0} pattern='{1}'" -f $rule.rule_id, $pattern)
        return $false
    }
}

# ---------------------------[ Apply Rules ]-----------------------------------
$kept = New-Object System.Collections.Generic.List[object]
$remain = New-Object System.Collections.Generic.List[object]
$audit = New-Object System.Collections.Generic.List[object]

$keepRuleHits = 0
$dropRuleHits = 0
$overrideHits = 0

foreach ($r in $rows) {

    # Overrides ALWAYS keep
    if (Is-OverrideKeep -r $r) {
        $overrideHits++

        $r2 = $r | Select-Object *
        Add-Member -InputObject $r2 -NotePropertyName keep_reason -NotePropertyValue "override_keep" -Force

        $kept.Add($r2) | Out-Null
        $audit.Add([PSCustomObject]@{
            site=$r.site; name=$r.name; xmltv_id=$r.xmltv_id; site_id=$r.site_id;
            action="KEEP"; rule_id="override_keep"; note="manual override"
        }) | Out-Null
        continue
    }

    $matchedKeep = $null
    $matchedDrop = $null

    foreach ($rule in $rules) {
        if (Match-Rule -row $r -rule $rule) {
            if (($rule.action ?? "").ToLowerInvariant() -eq "drop") {
                $matchedDrop = $rule
                break # drop wins immediately (unless override, already handled)
            }
            if (($rule.action ?? "").ToLowerInvariant() -eq "keep") {
                if (-not $matchedKeep) { $matchedKeep = $rule }
            }
        }
    }

    if ($matchedDrop) {
        $dropRuleHits++
        $remain.Add($r) | Out-Null
        $audit.Add([PSCustomObject]@{
            site=$r.site; name=$r.name; xmltv_id=$r.xmltv_id; site_id=$r.site_id;
            action="DROP"; rule_id=$matchedDrop.rule_id; note=$matchedDrop.note
        }) | Out-Null
        continue
    }

    if ($matchedKeep) {
        $keepRuleHits++

        $r2 = $r | Select-Object *
        Add-Member -InputObject $r2 -NotePropertyName keep_reason -NotePropertyValue ("rule_keep:" + $matchedKeep.rule_id) -Force

        $kept.Add($r2) | Out-Null
        $audit.Add([PSCustomObject]@{
            site=$r.site; name=$r.name; xmltv_id=$r.xmltv_id; site_id=$r.site_id;
            action="KEEP"; rule_id=$matchedKeep.rule_id; note=$matchedKeep.note
        }) | Out-Null
        continue
    }

    # Not selected yet ⇒ remaining
    $remain.Add($r) | Out-Null
    $audit.Add([PSCustomObject]@{
        site=$r.site; name=$r.name; xmltv_id=$r.xmltv_id; site_id=$r.site_id;
        action="REMAIN"; rule_id=""; note="no match"
    }) | Out-Null
}

Write-Log "INFO" ("Keep rule hits={0}" -f $keepRuleHits)
Write-Log "INFO" ("Drop rule hits={0}" -f $dropRuleHits)
Write-Log "INFO" ("Override keep hits={0}" -f $overrideHits)

Write-Log "INFO" ("Kept total={0}" -f $kept.Count)
Write-Log "INFO" ("Remaining total={0}" -f $remain.Count)

# ---------------------------[ Write Outputs ]---------------------------------
$kept | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $KeptCsv
$remain | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $RemainCsv
$audit | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $AuditCsv

$kept | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $KeptVer
$remain | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $RemainVer
$audit | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $AuditVer

Write-Log "OK" "Wrote kept CSV: $KeptCsv"
Write-Log "OK" "Wrote remaining CSV: $RemainCsv"
Write-Log "OK" "Wrote audit CSV: $AuditCsv"
Write-Log "OK" "Versioned outputs written for stamp=$Stamp"

Write-Log "INFO" "Done."
