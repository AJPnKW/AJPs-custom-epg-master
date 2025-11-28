<#
    Script:  step1_extract_all_sites_master_channels.ps1
    Version: 4.1.0
    Purpose:
      - Locate the best available master channels CSV (all_sites_master_channels.csv)
        from a set of candidate locations and snapshot it into:
          custom\baseline\all_sites_master_channels.csv         (stable)
          custom\baseline\versioned\all_sites_master_channels_YYYYMMDD_HHMMSS.csv
      - Avoid writing *anything* into custom\scripts.
      - Provide strong logging and simple QA so we don't regress into old bugs.

    Candidate search order (by folder, but final choice is by largest non-zero Length):
      1) custom\data\all_sites_master_channels.csv
      2) custom\scripts\all_sites_master_channels.csv
      3) custom\baseline\all_sites_master_channels.csv
#>

[CmdletBinding()]
param(
    [ValidateSet("Soft", "Hard")]
    [string]$Mode = "Soft"
)

# ------------------------------
# 1. Version & basic paths
# ------------------------------
$ScriptName    = "step1_extract_all_sites_master_channels.ps1"
$ScriptVersion = "4.1.0"

# current script folder = custom\scripts
$ScriptRoot   = Split-Path -Parent $PSCommandPath      # ...\custom\scripts
$CustomFolder = Split-Path -Parent $ScriptRoot         # ...\custom
$BasePath     = Split-Path -Parent $CustomFolder       # ...\AJPs-custom-epg-master\AJPs-custom-epg-master

# Key folders
$DataFolder      = Join-Path $CustomFolder "data"
$BaselineFolder  = Join-Path $CustomFolder "baseline"
$VersionedFolder = Join-Path $BaselineFolder "versioned"
$LogsFolder      = Join-Path $CustomFolder "logs"
$ScriptsFolder   = $ScriptRoot

# Output files
$Timestamp    = Get-Date -Format "yyyyMMdd_HHmmss"
$StableCsv    = Join-Path $BaselineFolder "all_sites_master_channels.csv"
$VersionedCsv = Join-Path $VersionedFolder ("all_sites_master_channels_{0}.csv" -f $Timestamp)
$LogFile      = Join-Path $LogsFolder "step1_extract_all_sites_master_channels.log"

# ------------------------------
# 2. Logging helper
# ------------------------------
function Write-Log {
    param(
        [Parameter(Mandatory=$true)][ValidateSet("DEBUG","INFO","WARN","ERROR","OK")]
        [string]$Level,
        [Parameter(Mandatory=$true)][string]$Message
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level, $Message
    Write-Host $line
    try {
        Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue
    } catch {
        Write-Host "[{0}][WARN] Failed to write to log file: {1}" -f $ts, $_.Exception.Message
    }
}

# ------------------------------
# 3. Ensure required folders exist
# ------------------------------
foreach ($folder in @($BaselineFolder, $VersionedFolder, $LogsFolder)) {
    if (-not (Test-Path $folder)) {
        New-Item -ItemType Directory -Path $folder -Force | Out-Null
        Write-Log -Level "DEBUG" -Message "Created folder: $folder"
    } else {
        Write-Log -Level "DEBUG" -Message "Folder exists: $folder"
    }
}

# ------------------------------
# 4. Start banner
# ------------------------------
Write-Log -Level "INFO" -Message "Starting $ScriptName v$ScriptVersion (Mode=$Mode)"
Write-Log -Level "INFO" -Message "BasePath=$BasePath"
Write-Log -Level "INFO" -Message "CustomFolder=$CustomFolder"
Write-Log -Level "INFO" -Message "DataFolder=$DataFolder"
Write-Log -Level "INFO" -Message "BaselineFolder=$BaselineFolder"
Write-Log -Level "INFO" -Message "VersionedFolder=$VersionedFolder"
Write-Log -Level "INFO" -Message "ScriptsFolder=$ScriptsFolder"

# ------------------------------
# 5. Discover best source CSV
# ------------------------------
$candidatePaths = @(
    Join-Path $DataFolder    "all_sites_master_channels.csv",
    Join-Path $ScriptsFolder "all_sites_master_channels.csv",
    Join-Path $BaselineFolder "all_sites_master_channels.csv"
)

Write-Log -Level "DEBUG" -Message "Scanning candidate source CSV locations:"
foreach ($c in $candidatePaths) {
    Write-Log -Level "DEBUG" -Message "  Candidate: $c"
}

$candidateInfos = @()

foreach ($path in $candidatePaths) {
    if (Test-Path $path) {
        try {
            $info = Get-Item $path
            $len  = [int64]$info.Length
            Write-Log -Level "DEBUG" -Message "  Found candidate: $path (Length=$len)"
            if ($len -gt 0) {
                $candidateInfos += $info
            } else {
                Write-Log -Level "WARN" -Message "  Candidate exists but is empty (0 bytes): $path"
            }
        } catch {
            Write-Log -Level "WARN" -Message "  Failed to inspect candidate: $path : $($_.Exception.Message)"
        }
    } else {
        Write-Log -Level "DEBUG" -Message "  Candidate not found: $path"
    }
}

if (-not $candidateInfos -or $candidateInfos.Count -eq 0) {
    Write-Log -Level "ERROR" -Message "No non-empty all_sites_master_channels.csv found in data/scripts/baseline."
    if ($Mode -eq "Hard") {
        Write-Log -Level "ERROR" -Message "Mode=Hard → failing pipeline."
        exit 1
    } else {
        Write-Log -Level "WARN" -Message "Mode=Soft → exiting Step 1 gracefully with no output."
        exit 0
    }
}

# pick the largest non-zero candidate as source-of-truth
$BestSourceInfo = $candidateInfos | Sort-Object -Property Length -Descending | Select-Object -First 1
$SourceCsv = $BestSourceInfo.FullName
$SourceLen = $BestSourceInfo.Length

Write-Log -Level "OK" -Message "Selected source CSV: $SourceCsv (Length=$SourceLen)"
Write-Log -Level "INFO" -Message "StableCsv=$StableCsv"
Write-Log -Level "INFO" -Message "VersionedCsv=$VersionedCsv"

# ------------------------------
# 6. Load source rows
# ------------------------------
try {
    $SourceRows = Import-Csv -Path $SourceCsv
} catch {
    Write-Log -Level "ERROR" -Message "Failed to Import-Csv from $SourceCsv : $($_.Exception.Message)"
    if ($Mode -eq "Hard") { exit 1 } else { exit 0 }
}

$sourceCount = if ($SourceRows) { $SourceRows.Count } else { 0 }
Write-Log -Level "INFO" -Message "Loaded $sourceCount rows from selected source CSV."

if ($sourceCount -eq 0) {
    Write-Log -Level "WARN" -Message "Selected source CSV has headers but no data rows. Nothing to snapshot."
    if ($Mode -eq "Hard") {
        Write-Log -Level "ERROR" -Message "Mode=Hard → failing pipeline."
        exit 1
    } else {
        exit 0
    }
}

# ------------------------------
# 7. Write stable + versioned copies
# ------------------------------
try {
    $SourceRows | Export-Csv -NoTypeInformation -Path $StableCsv
    Write-Log -Level "OK" -Message "Wrote stable baseline CSV: $StableCsv"
} catch {
    Write-Log -Level "ERROR" -Message "Failed to write stable CSV to $StableCsv : $($_.Exception.Message)"
    if ($Mode -eq "Hard") { exit 1 }
}

try {
    $SourceRows | Export-Csv -NoTypeInformation -Path $VersionedCsv
    Write-Log -Level "OK" -Message "Wrote versioned baseline CSV: $VersionedCsv"
} catch {
    Write-Log -Level "ERROR" -Message "Failed to write versioned CSV to $VersionedCsv : $($_.Exception.Message)"
    if ($Mode -eq "Hard") { exit 1 }
}

# ------------------------------
# 8. QA check: re-load stable and compare row counts
# ------------------------------
try {
    $StableRows = Import-Csv -Path $StableCsv
    $stableCount = if ($StableRows) { $StableRows.Count } else { 0 }

    if ($stableCount -ne $sourceCount) {
        Write-Log -Level "WARN" -Message "Row count mismatch after export: source=$sourceCount, stable=$stableCount"
    } else {
        Write-Log -Level "OK" -Message "Row count QA passed: $sourceCount rows."
    }
} catch {
    Write-Log -Level "WARN" -Message "Could not re-load stable CSV for QA: $($_.Exception.Message)"
}

Write-Log -Level "INFO" -Message "Step 1 complete."
Write-Log -Level "INFO" -Message "Finished $ScriptName v$ScriptVersion"
