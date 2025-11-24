<# 
Script Name: step2_select_core_channels.ps1
Purpose: Step 2. Select the "core keep" channel set from the master dataset
         using positive selection (keep-everything-after-drops) plus persistent overrides.
Author: ChatGPT for Andrew Pearen
Created: 2025-11-23
Last Updated: 2025-11-23
Version: 2.3

Inputs:
- custom\data\all_sites_master_channels.csv
- custom\rules\step2_select_core_rules.csv
- custom\rules\overrides_keep.csv
- custom\rules\overrides_drop.csv (optional)

Outputs:
- custom\baseline\step2_core_keep.csv
- custom\baseline\step2_core_dropped.csv
- custom\baseline\step2_core_remaining.csv
- custom\baseline\versioned\step2_core_keep_<stamp>.csv
- custom\baseline\versioned\step2_core_dropped_<stamp>.csv

Run:
PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step2_select_core_channels.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --------------------------- Paths ---------------------------
$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$CustomPath = Join-Path $BasePath "custom"

$InputCSV   = Join-Path $CustomPath "data\all_sites_master_channels.csv"
$RulesCSV   = Join-Path $CustomPath "rules\step2_select_core_rules.csv"
$KeepCSV    = Join-Path $CustomPath "rules\overrides_keep.csv"
$DropCSV    = Join-Path $CustomPath "rules\overrides_drop.csv"

$BaselinePath = Join-Path $CustomPath "baseline"
$VersionPath  = Join-Path $BaselinePath "versioned"
$Stamp        = (Get-Date).ToString("yyyyMMdd_HHmmss")

$OutKeep      = Join-Path $BaselinePath "step2_core_keep.csv"
$OutDrop      = Join-Path $BaselinePath "step2_core_dropped.csv"
$OutRemain    = Join-Path $BaselinePath "step2_core_remaining.csv"

$OutKeepV     = Join-Path $VersionPath "step2_core_keep_$Stamp.csv"
$OutDropV     = Join-Path $VersionPath "step2_core_dropped_$Stamp.csv"

$LogPath = Join-Path $CustomPath "logs"
$LogFile = Join-Path $LogPath "step2_select_core_channels.log"

# --------------------------- Logging ---------------------------
function Write-Log {
    param([string]$Level, [string]$Message)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$ts][$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

# --------------------------- Helpers ---------------------------
function Normalize-Key {
    param($r)
    # strict dedupe key; keeps identical rows only
    return (
        (($r.site ?? "") + "|" +
         ($r.site_id ?? "") + "|" +
         ($r.xmltv_id ?? "") + "|" +
         ($r.name ?? "") + "|" +
         ($r.lang ?? "") + "|" +
         ($r.country ?? "") + "|" +
         ($r.hd_flag ?? "")
        ).ToLower().Trim()
    )
}

function Match-Rule {
    param($row, $rule)
    $field = $rule.field
    $regex = $rule.regex
    if ([string]::IsNullOrWhiteSpace($field) -or [string]::IsNullOrWhiteSpace($regex)) { return $false }

    $val = ""
    if ($row.PSObject.Properties.Name -contains $field) {
        $val = ($row.$field ?? "").ToString()
    }
    if ($rule.countries -and $rule.countries.Trim() -ne "") {
        $allowed = $rule.countries.Split("|") | ForEach-Object { $_.Trim().ToUpper() }
        if ($allowed -notcontains (($row.country ?? "").ToUpper())) { return $false }
    }
    return ($val -match $regex)
}

# --------------------------- Main ---------------------------
try {
    if (!(Test-Path $LogPath)) { New-Item -ItemType Directory -Force -Path $LogPath | Out-Null }
    if (!(Test-Path $VersionPath)) { New-Item -ItemType Directory -Force -Path $VersionPath | Out-Null }

    Write-Log "INFO" "Starting step2_select_core_channels.ps1 v2.3"
    Write-Log "INFO" "InputCSV=$InputCSV"
    Write-Log "INFO" "RulesCSV=$RulesCSV"
    Write-Log "INFO" "OverridesKeep=$KeepCSV"
    Write-Log "INFO" "OverridesDrop=$DropCSV"

    if (!(Test-Path $InputCSV)) { throw "Missing $InputCSV" }
    if (!(Test-Path $RulesCSV)) { throw "Missing $RulesCSV" }

    $raw = Import-Csv $InputCSV
    Write-Log "INFO" ("Loaded raw rows=" + $raw.Count)

    # -------- Deduplicate identical rows --------
    $seen = @{}
    $deduped = foreach ($r in $raw) {
        $k = Normalize-Key $r
        if (!$seen.ContainsKey($k)) {
            $seen[$k] = $true
            $r
        }
    }
    $dupsRemoved = $raw.Count - $deduped.Count
    Write-Log "INFO" ("Deduped rows=" + $deduped.Count + " (duplicates removed=" + $dupsRemoved + ")")

    # -------- Load rules --------
    $rules = Import-Csv $RulesCSV
    $dropRules = $rules | Where-Object { $_.action -eq "DROP" }
    $keepRules = $rules | Where-Object { $_.action -eq "KEEP" }  # reserved for later steps

    # -------- Apply DROP rules (core negatives) --------
    $kept = New-Object System.Collections.Generic.List[object]
    $dropped = New-Object System.Collections.Generic.List[object]

    foreach ($r in $deduped) {
        $hit = $null
        foreach ($rule in $dropRules) {
            if (Match-Rule $r $rule) { $hit = $rule; break }
        }

        if ($hit) {
            $r | Add-Member NoteProperty reason $hit.rule_id -Force
            $dropped.Add($r)
        } else {
            $kept.Add($r)
        }
    }

    Write-Log "INFO" ("Core kept after drops=" + $kept.Count)
    Write-Log "INFO" ("Core dropped=" + $dropped.Count)

    # -------- Apply Overrides DROP (always drop) --------
    if (Test-Path $DropCSV) {
        $od = Import-Csv $DropCSV
        if ($od.Count -gt 0) {
            $dropKeys = @{}
            foreach ($x in $od) { $dropKeys[(Normalize-Key $x)] = $true }

            $kept2 = New-Object System.Collections.Generic.List[object]
            foreach ($r in $kept) {
                if ($dropKeys.ContainsKey((Normalize-Key $r))) {
                    $r | Add-Member NoteProperty reason "override_drop" -Force
                    $dropped.Add($r)
                } else {
                    $kept2.Add($r)
                }
            }
            $kept = $kept2
            Write-Log "INFO" ("Applied overrides_drop. Kept now=" + $kept.Count)
        }
    }

    # -------- Apply Overrides KEEP (your manual keep list, persistent) --------
    if (Test-Path $KeepCSV) {
        $ok = Import-Csv $KeepCSV
        if ($ok.Count -gt 0) {
            $keepKeys = @{}
            foreach ($x in $ok) { $keepKeys[(Normalize-Key $x)] = $true }

            $keptKeysAlready = @{}
            foreach ($r in $kept) { $keptKeysAlready[(Normalize-Key $r)] = $true }

            $readded = 0
            $stillDropped = New-Object System.Collections.Generic.List[object]

            foreach ($r in $dropped) {
                $k = Normalize-Key $r
                if ($keepKeys.ContainsKey($k) -and -not $keptKeysAlready.ContainsKey($k)) {
                    $r | Add-Member NoteProperty reason "override_keep" -Force
                    $kept.Add($r)
                    $readded++
                } else {
                    $stillDropped.Add($r)
                }
            }
            $dropped = $stillDropped
            Write-Log "INFO" ("Applied overrides_keep. Readded=" + $readded + " Kept now=" + $kept.Count)
        }
    }

    # Remaining = raw minus final kept
    $finalKeys = @{}
    foreach ($r in $kept) { $finalKeys[(Normalize-Key $r)] = $true }

    $remaining = foreach ($r in $deduped) {
        if (-not $finalKeys.ContainsKey((Normalize-Key $r))) { $r }
    }

    # -------- Write outputs --------
    $kept      | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutKeep
    $dropped   | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutDrop
    $remaining | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutRemain

    $kept    | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutKeepV
    $dropped | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutDropV

    Write-Log "OK" "Wrote core keep: $OutKeep"
    Write-Log "OK" "Wrote dropped audit: $OutDrop"
    Write-Log "OK" "Wrote remaining: $OutRemain"
    Write-Log "OK" "Versioned outputs written stamp=$Stamp"

    Write-Log "OK" "Step 2 complete."
}
catch {
    Write-Log "ERROR" ("Step 2 failed: " + $_.Exception.Message)
    throw
}
finally {
    Write-Log "INFO" "Finished step2_select_core_channels.ps1"
}
