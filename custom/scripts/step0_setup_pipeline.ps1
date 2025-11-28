<#
    Script Name : step0_setup_pipeline.ps1
    Purpose     : Bootstrap the custom EPG pipeline:
                  - Ensure standard folder structure exists
                  - Ensure single source-of-truth master CSV is in custom\data
                  - Migrate any legacy copies of all_sites_master_channels.csv
                  - Never write data into custom\scripts

    Author      : ChatGPT + Andrew
    Version     : 2.0.0
    Created     : 2025-11-25
    Last Update : 2025-11-25

    Usage       :
        # Soft mode (default) – non-destructive, no overwrites
        .\step0_setup_pipeline.ps1

        # Hard mode – will overwrite the target with a provided source
        .\step0_setup_pipeline.ps1 -Mode Hard -SourceCsv "C:\path\to\all_sites_master_channels.csv"
#>

[CmdletBinding()]
param(
    [ValidateSet("Soft","Hard")]
    [string]$Mode = "Soft",

    # Optional path to a "clean" upstream master CSV
    [string]$SourceCsv
)

# ---------------------------
# Helpers
# ---------------------------

function Write-Log {
    param(
        [Parameter(Mandatory)][ValidateSet("DEBUG","INFO","WARN","ERROR","OK")]
        [string]$Level,
        [Parameter(Mandatory)][string]$Message
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level, $Message
    Write-Host $line
    if ($script:LogFile) {
        Add-Content -Path $script:LogFile -Value $line
    }
}

function Ensure-Folder {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
        Write-Log -Level "INFO" -Message "Created folder: $Path"
    } else {
        Write-Log -Level "DEBUG" -Message "Folder exists: $Path"
    }
}

# ---------------------------
# Paths
# ---------------------------

$BasePath        = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$CustomPath      = Join-Path $BasePath "custom"
$DataFolder      = Join-Path $CustomPath "data"
$BaselineFolder  = Join-Path $CustomPath "baseline"
$VersionedFolder = Join-Path $BaselineFolder "versioned"
$LogsFolder      = Join-Path $CustomPath "logs"
$RulesFolder     = Join-Path $CustomPath "rules"

$TargetMasterCsv = Join-Path $DataFolder "all_sites_master_channels.csv"

# Log file
$script:LogFile = Join-Path $LogsFolder "step0_setup_pipeline.log"
Ensure-Folder -Path $LogsFolder

Write-Log -Level "INFO" -Message "Starting step0_setup_pipeline.ps1 v2.0.0 (Mode=$Mode)"
Write-Log -Level "INFO" -Message "BasePath=$BasePath"
Write-Log -Level "INFO" -Message "CustomPath=$CustomPath"
Write-Log -Level "INFO" -Message "DataFolder=$DataFolder"
Write-Log -Level "INFO" -Message "BaselineFolder=$BaselineFolder"
Write-Log -Level "INFO" -Message "VersionedFolder=$VersionedFolder"

# ---------------------------
# Ensure folder structure
# ---------------------------

Ensure-Folder -Path $CustomPath
Ensure-Folder -Path $DataFolder
Ensure-Folder -Path $BaselineFolder
Ensure-Folder -Path $VersionedFolder
Ensure-Folder -Path $RulesFolder

# ---------------------------
# Migrate any legacy copies of master CSV
# ---------------------------

$LegacyInScripts  = Join-Path (Join-Path $CustomPath "scripts") "all_sites_master_channels.csv"
$LegacyInBaseline = Join-Path $BaselineFolder "all_sites_master_channels.csv"

if (Test-Path -LiteralPath $LegacyInScripts) {
    Write-Log -Level "WARN" -Message "Found legacy master CSV in scripts: $LegacyInScripts"
    if (-not (Test-Path -LiteralPath $TargetMasterCsv)) {
        Move-Item -LiteralPath $LegacyInScripts -Destination $TargetMasterCsv
        Write-Log -Level "OK" -Message "Moved legacy scripts copy to data: $TargetMasterCsv"
    } else {
        Write-Log -Level "WARN" -Message "Target master already exists; leaving legacy scripts copy in place for manual review."
    }
}

if (Test-Path -LiteralPath $LegacyInBaseline) {
    Write-Log -Level "WARN" -Message "Found baseline copy of master CSV: $LegacyInBaseline"
    Write-Log -Level "INFO" -Message "Baseline copy is treated as output. It will NOT be moved automatically."
}

# ---------------------------
# If target already exists
# ---------------------------

if (Test-Path -LiteralPath $TargetMasterCsv) {
    $item = Get-Item -LiteralPath $TargetMasterCsv
    Write-Log -Level "OK" -Message ("Existing master CSV found in data: {0} (Length={1})" -f $item.FullName, $item.Length)

    if ($Mode -eq "Hard" -and $SourceCsv) {
        if (-not (Test-Path -LiteralPath $SourceCsv)) {
            Write-Log -Level "ERROR" -Message "Hard mode requested but SourceCsv not found: $SourceCsv"
            throw "SourceCsv not found."
        }
        Copy-Item -LiteralPath $SourceCsv -Destination $TargetMasterCsv -Force
        $item = Get-Item -LiteralPath $TargetMasterCsv
        Write-Log -Level "OK" -Message ("Hard mode: Overwrote master CSV from SourceCsv. New Length={0}" -f $item.Length)
    } else {
        Write-Log -Level "INFO" -Message "Soft mode and master already present. No overwrite performed."
    }

    Write-Log -Level "INFO" -Message "step0_setup_pipeline.ps1 complete."
    return
}

# ---------------------------
# Target does not exist yet
# ---------------------------

Write-Log -Level "WARN" -Message "Master CSV not present at $TargetMasterCsv"

if ($SourceCsv) {
    if (-not (Test-Path -LiteralPath $SourceCsv)) {
        Write-Log -Level "ERROR" -Message "Provided SourceCsv does not exist: $SourceCsv"
        throw "SourceCsv does not exist."
    }

    Copy-Item -LiteralPath $SourceCsv -Destination $TargetMasterCsv -Force
    $item = Get-Item -LiteralPath $TargetMasterCsv
    Write-Log -Level "OK" -Message ("Copied SourceCsv to master location. Length={0}" -f $item.Length)
    Write-Log -Level "INFO" -Message "step0_setup_pipeline.ps1 complete."
    return
}

# No source CSV provided and no legacy copy to migrate
Write-Log -Level "ERROR" -Message @"
No master CSV could be found or created.

Expected master path:
  $TargetMasterCsv

How to fix:
  - Restore a clean 'all_sites_master_channels.csv' from Git:
      git checkout main -- custom/data/all_sites_master_channels.csv
  - OR download a known-good copy from GitHub and save it to that path.
  - Then re-run:
      .\step0_setup_pipeline.ps1

This script intentionally does NOT attempt to generate the master from XML/APIs,
to avoid reintroducing previous parsing bugs.
"@

if ($Mode -eq "Hard") {
    throw "Master CSV missing and no SourceCsv provided in Hard mode."
} else {
    Write-Log -Level "WARN" -Message "Soft mode: exiting without master CSV. Downstream steps will see an empty baseline."
}
