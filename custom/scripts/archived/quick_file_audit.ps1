<#
Script Name: quick_file_audit.ps1
Purpose    : Fast audit of key pipeline files: presence, location, timestamps,
             row counts, required columns, blank critical fields, BOM check.
Author     : ChatGPT for Andrew Pearen
Created    : 2025-11-23
Updated    : 2025-11-23
Version    : 1.0

Run:
  PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\quick_file_audit.ps1

Inputs:
  LOCAL FILES ONLY — reads from custom\data and custom\output.

Outputs:
  custom\output\quick_file_audit_report.csv
Logs:
  custom\logs\quick_file_audit.log
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------- paths ----------------
$BasePath   = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$DataPath   = Join-Path $BasePath "custom\data"
$OutputPath = Join-Path $BasePath "custom\output"
$LogPath    = Join-Path $BasePath "custom\logs"
$ReportOut  = Join-Path $OutputPath "quick_file_audit_report.csv"
$LogFile    = Join-Path $LogPath "quick_file_audit.log"

New-Item -ItemType Directory -Force -Path $DataPath, $OutputPath, $LogPath | Out-Null

function Write-Log {
    param([string]$Msg, [string]$Level="INFO")
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$ts][$Level] $Msg"
    Add-Content -Path $LogFile -Value $line
    Write-Host $line
}

function Get-BomType {
    param([string]$Path)
    try {
        $bytes = [System.IO.File]::ReadAllBytes($Path)
        if($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF){
            return "UTF8_BOM"
        }
        if($bytes.Length -ge 2 -and $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE){
            return "UTF16_LE_BOM"
        }
        if($bytes.Length -ge 2 -and $bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF){
            return "UTF16_BE_BOM"
        }
        return "NONE"
    } catch {
        return "UNKNOWN"
    }
}

function Audit-Csv {
    param(
        [string]$Path,
        [string[]]$RequiredCols = @(),
        [string[]]$CriticalCols = @()
    )

    $result = [ordered]@{
        file_name           = Split-Path $Path -Leaf
        expected_path       = $Path
        exists              = $false
        last_write_time     = ""
        size_bytes          = 0
        bom                 = ""
        row_count           = 0
        has_required_cols   = $true
        missing_cols        = ""
        extra_cols          = ""
        blank_critical      = ""
        sample_rows         = ""
        notes               = ""
    }

    if(!(Test-Path $Path)){
        $result.exists = $false
        $result.notes  = "MISSING"
        return [pscustomobject]$result
    }

    $fi = Get-Item $Path
    $result.exists          = $true
    $result.last_write_time = $fi.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
    $result.size_bytes      = $fi.Length
    $result.bom             = Get-BomType $Path

    try {
        $rows = Import-Csv $Path
    } catch {
        $result.notes = "FAILED_IMPORT_CSV: $($_.Exception.Message)"
        return [pscustomobject]$result
    }

    $result.row_count = $rows.Count

    # Column validation
    $cols = @()
    if($rows.Count -gt 0){
        $cols = $rows[0].PSObject.Properties.Name
    }

    $missing = @()
    foreach($c in $RequiredCols){
        if($cols -notcontains $c){ $missing += $c }
    }
    if($missing.Count -gt 0){
        $result.has_required_cols = $false
        $result.missing_cols = ($missing -join "|")
    }

    $extra = @()
    foreach($c in $cols){
        if($RequiredCols.Count -gt 0 -and $RequiredCols -notcontains $c){
            $extra += $c
        }
    }
    if($extra.Count -gt 0){
        $result.extra_cols = ($extra -join "|")
    }

    # Blank critical fields
    $blankStats = @()
    foreach($cc in $CriticalCols){
        if($cols -contains $cc){
            $blankCount = ($rows | Where-Object { -not $_.$cc -or ($_.${cc}).ToString().Trim() -eq "" }).Count
            $blankStats += "$cc=$blankCount"
        } else {
            $blankStats += "$cc=COL_MISSING"
        }
    }
    if($blankStats.Count -gt 0){
        $result.blank_critical = ($blankStats -join ";")
    }

    # Sample rows (first 3)
    $samples = @()
    foreach($r in ($rows | Select-Object -First 3)){
        $s = ($cols | ForEach-Object { "$_=$($r.$_)" }) -join ", "
        $samples += "{" + $s + "}"
    }
    $result.sample_rows = ($samples -join " || ")

    # size warnings
    if($result.size_bytes -lt 50){
        $result.notes += " VERY_SMALL_FILE;"
    }
    if($result.row_count -eq 0){
        $result.notes += " ZERO_ROWS;"
    }

    return [pscustomobject]$result
}

Write-Log "Starting quick_file_audit.ps1"
Write-Log "BasePath=$BasePath"
Write-Log "DataPath=$DataPath"
Write-Log "OutputPath=$OutputPath"
Write-Log "LogPath=$LogPath"

# ---------------- file manifest ----------------
$manifest = @(
    @{
        path = Join-Path $OutputPath "Draft-Keep.raw.csv"
        required = @("tag","site","name","xmltv_id","lang","site_id","source_file")
        critical = @("site","name","site_id","xmltv_id")
    },
    @{
        path = Join-Path $OutputPath "Draft-Keep.enriched.csv"
        required = @("tag","site","name","xmltv_id","lang","site_id","source_file")
        critical = @("site","name","site_id","xmltv_id")
    },
    @{
        path = Join-Path $OutputPath "Draft-Keep.still_missing.csv"
        required = @("tag","site","name","xmltv_id","lang","site_id","source_file")
        critical = @("site","name","site_id","xmltv_id")
    },
    @{
        path = Join-Path $OutputPath "matched_channels.csv"
        required = @("name","site","site_id","xmltv_id","country","lang","source_file","match_reason")
        critical = @("name","site","site_id")
    },
    @{
        path = Join-Path $OutputPath "ci_bad_rows.csv"
        required = @()
        critical = @()
    },
    @{
        path = Join-Path $DataPath "keep_seeds.csv"
        required = @("display_name","country","match_hint")
        critical = @("display_name","country","match_hint")
    },
    @{
        path = Join-Path $OutputPath "consolidated_inventory.csv"
        required = @("site","name","xmltv_id","lang","site_id","source_file","country")
        critical = @("site","name","site_id")
    }
)

$report = New-Object System.Collections.Generic.List[object]

foreach($m in $manifest){
    $r = Audit-Csv -Path $m.path -RequiredCols $m.required -CriticalCols $m.critical
    $report.Add($r) | Out-Null
    Write-Log ("Audited {0} exists={1} rows={2} bom={3}" -f $r.file_name,$r.exists,$r.row_count,$r.bom)
}

$report | Export-Csv $ReportOut -NoTypeInformation -Encoding UTF8
Write-Log "Wrote audit report: $ReportOut"
Write-Log "Done."
