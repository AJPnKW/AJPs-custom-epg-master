<#
.SYNOPSIS
  Step 2 - Enrich master channel inventory with preferred list and IPTV-org metadata.

.DESCRIPTION
  - Reads baseline channel inventory from Step 1:
      custom\baseline\all_sites_master_channels.csv
  - Reads preferred / scoped channel list:
      custom\rules\prefered-scoped-channels.csv
  - Optionally enriches with IPTV-org channel database:
      custom\data\iptv-org\channels.csv
  - Optionally uses category excludes:
      custom\rules\exclude_categories.csv

  Produces enriched outputs:
      custom\baseline\all_sites_master_channels_enriched.csv
      custom\baseline\versioned\all_sites_master_channels_enriched_YYYYMMDD_HHMMSS.csv

.NOTES
  Script Name : step2_enrich_with_preferred_channels.ps1
  Version     : 1.0.0
  Author      : ChatGPT for Andrew J. Pearen
  Created     : 2025-11-25
  Purpose     : PowerShell-only Step 2 for AJPs-custom-epg-master pipeline.

.EXAMPLE
  # Soft mode (warnings only, no hard stops)
  .\step2_enrich_with_preferred_channels.ps1 -Mode Soft

  # Strict mode (throws if key inputs are missing)
  .\step2_enrich_with_preferred_channels.ps1 -Mode Strict

#>

[CmdletBinding()]
param(
    [ValidateSet("Soft","Strict")]
    [string]$Mode = "Soft"
)

# ---------------------------
# 1. Script metadata & paths
# ---------------------------

$ScriptName    = "step2_enrich_with_preferred_channels.ps1"
$ScriptVersion = "1.0.0"

# Example absolute path to run, adjust if needed:
# PowerShell: C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step2_enrich_with_preferred_channels.ps1

# PSScriptRoot = ...\custom\scripts
$ScriptsRoot = $PSScriptRoot
$CustomRoot  = Split-Path -Parent $ScriptsRoot      # ...\custom
$BasePath    = Split-Path -Parent $CustomRoot       # project root

$BaselineDir       = Join-Path $CustomRoot "baseline"
$BaselineStableCsv = Join-Path $BaselineDir "all_sites_master_channels.csv"

$RulesDir                 = Join-Path $CustomRoot "rules"
$PreferredScopedCsv       = Join-Path $RulesDir "prefered-scoped-channels.csv"
$ExcludeCategoriesCsvPath = Join-Path $RulesDir "exclude_categories.csv"

$IptvOrgDir        = Join-Path $CustomRoot "data\iptv-org"
$IptvChannelsCsv   = Join-Path $IptvOrgDir "channels.csv"

$LogsDir           = Join-Path $CustomRoot "logs"
$VersionedDir      = Join-Path $BaselineDir "versioned"

$Timestamp         = Get-Date -Format "yyyyMMdd_HHmmss"
$EnrichedStableCsv = Join-Path $BaselineDir "all_sites_master_channels_enriched.csv"
$EnrichedVersioned = Join-Path $VersionedDir ("all_sites_master_channels_enriched_{0}.csv" -f $Timestamp)

# Ensure required folders exist
$null = New-Item -ItemType Directory -Path $BaselineDir -ErrorAction SilentlyContinue
$null = New-Item -ItemType Directory -Path $LogsDir     -ErrorAction SilentlyContinue
$null = New-Item -ItemType Directory -Path $VersionedDir -ErrorAction SilentlyContinue

# Logging
$script:LogFile = Join-Path $LogsDir "step2_enrich_with_preferred_channels.log"

function Write-Log {
    param(
        [ValidateSet("DEBUG","INFO","WARN","ERROR","OK")]
        [string]$Level,
        [string]$Message
    )

    $ts   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}][{1}] {2}" -f $ts, $Level, $Message

    # Console: INFO+ and WARN/ERROR; DEBUG only in console if needed
    if ($Level -in @("INFO","WARN","ERROR","OK")) {
        Write-Host $line
    }

    # Log file (all levels)
    try {
        Add-Content -Path $script:LogFile -Value $line -ErrorAction SilentlyContinue
    } catch {
        # If logging fails, don't break the script
        Write-Host "[{0}][WARN] Failed to write log file: {1}" -f $ts, $_.Exception.Message
    }
}

Write-Log -Level INFO -Message ("Starting {0} v{1} (Mode={2})" -f $ScriptName, $ScriptVersion, $Mode)
Write-Log -Level INFO -Message ("BasePath={0}" -f $BasePath)
Write-Log -Level INFO -Message ("BaselineStableCsv={0}" -f $BaselineStableCsv)
Write-Log -Level INFO -Message ("PreferredScopedCsv={0}" -f $PreferredScopedCsv)
Write-Log -Level INFO -Message ("IptvChannelsCsv={0}" -f $IptvChannelsCsv)
Write-Log -Level INFO -Message ("ExcludeCategoriesCsvPath={0}" -f $ExcludeCategoriesCsvPath)
Write-Log -Level INFO -Message ("EnrichedStableCsv={0}" -f $EnrichedStableCsv)
Write-Log -Level INFO -Message ("EnrichedVersioned={0}" -f $EnrichedVersioned)

# -----------------------------------
# 2. Helper: Safe CSV import wrapper
# -----------------------------------

function Import-CsvSafe {
    param(
        [string]$Path,
        [string]$Description
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        Write-Log -Level WARN -Message ("Missing {0}: {1}" -f $Description, $Path)
        return @()
    }

    try {
        $rows = Import-Csv -LiteralPath $Path
        $count = 0
        if ($rows) {
            # collection, not $null
            if ($rows -is [System.Array]) {
                $count = $rows.Count
            } else {
                $count = 1
                $rows  = @($rows)
            }
        }
        Write-Log -Level INFO -Message ("Loaded {0} rows from {1}" -f $count, $Description)
        return $rows
    }
    catch {
        Write-Log -Level ERROR -Message ("Failed to import {0} from {1}: {2}" -f $Description, $Path, $_.Exception.Message)
        if ($Mode -eq "Strict") {
            throw
        }
        return @()
    }
}

# ---------------------------------------
# 3. Load inputs (baseline / preferred)
# ---------------------------------------

$Baseline = Import-CsvSafe -Path $BaselineStableCsv -Description "baseline master channels (Step1 output)"
if (($Baseline -eq $null) -or ($Baseline.Count -eq 0)) {
    Write-Log -Level WARN -Message "Baseline master channels is empty or missing. Enrichment will still run but will only emit rows we can construct from preferred/db data."
}

$PreferredScoped = Import-CsvSafe -Path $PreferredScopedCsv -Description "preferred scoped channels"
if (($PreferredScoped -eq $null) -or ($PreferredScoped.Count -eq 0)) {
    Write-Log -Level WARN -Message "No preferred scoped channels found. IsPreferred will be false for all baseline rows."
}

$IptvChannels = Import-CsvSafe -Path $IptvChannelsCsv -Description "IPTV-org channels database"
if (($IptvChannels -eq $null) -or ($IptvChannels.Count -eq 0)) {
    Write-Log -Level WARN -Message "IPTV-org channels database not available or empty. Enrichment will rely only on baseline and preferred lists."
}

$ExcludeCategories = Import-CsvSafe -Path $ExcludeCategoriesCsvPath -Description "exclude categories"
$ExcludedCategorySet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

foreach ($row in $ExcludeCategories) {
    if ($row -and $row.category) {
        [void]$ExcludedCategorySet.Add($row.category.Trim())
    }
}
if ($ExcludedCategorySet.Count -gt 0) {
    Write-Log -Level INFO -Message ("Loaded {0} excluded categories" -f $ExcludedCategorySet.Count)
} else {
    Write-Log -Level INFO -Message "No excluded categories configured (or file missing/empty)."
}

# ------------------------------------------------
# 4. Build lookups for IPTV channels & Preferred
# ------------------------------------------------

# Helper: normalize strings for matching
function Normalize-Name {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return "" }
    $trim = $Value.Trim()
    # lower-case, collapse multiple spaces
    $lower = $trim.ToLowerInvariant()
    $norm  = ($lower -replace '\s+', ' ')
    return $norm
}

# IPTV: lookup by ID and by normalized name+country
$iptvById = @{}
$iptvByNameCountry = @{}  # key: "<normName>|<country>"

foreach ($ch in $IptvChannels) {
    if (-not $ch) { continue }

    $id = $ch.id
    if ($id) {
        $lowerId = $id.ToString().ToLowerInvariant()
        if (-not $iptvById.ContainsKey($lowerId)) {
            $iptvById[$lowerId] = $ch
        }
    }

    $nameNorm   = Normalize-Name -Value $ch.name
    $countryKey = ($ch.country ? $ch.country.Trim() : "")
    $key        = "{0}|{1}" -f $nameNorm, $countryKey

    if (-not $iptvByNameCountry.ContainsKey($key)) {
        $iptvByNameCountry[$key] = @()
    }
    $iptvByNameCountry[$key] += $ch

    # Also index alt_names (if any)
    if ($ch.alt_names) {
        $alts = $ch.alt_names -split ';'
        foreach ($alt in $alts) {
            $altNorm = Normalize-Name -Value $alt
            if (-not [string]::IsNullOrWhiteSpace($altNorm)) {
                $akey = "{0}|{1}" -f $altNorm, $countryKey
                if (-not $iptvByNameCountry.ContainsKey($akey)) {
                    $iptvByNameCountry[$akey] = @()
                }
                $iptvByNameCountry[$akey] += $ch
            }
        }
    }
}

Write-Log -Level INFO -Message ("IPTV-org by-id entries: {0}" -f $iptvById.Count)
Write-Log -Level INFO -Message ("IPTV-org by-name-country keys: {0}" -f $iptvByNameCountry.Count)

# Preferred: set of IDs and/or names
$preferredIdSet   = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$preferredNameSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

foreach ($p in $PreferredScoped) {
    if (-not $p) { continue }

    # prefered flag may be "Y" or something non-empty
    $isPreferredRow = $false
    foreach ($key in $p.PSObject.Properties.Name) {
        if ($key -match 'prefer') {
            $val = $p.$key
            if ($val -and $val.ToString().Trim().ToUpperInvariant() -eq 'Y') {
                $isPreferredRow = $true
                break
            }
        }
    }

    if (-not $isPreferredRow) {
        # If there is no explicit Prefered column, treat every row as "preferred"
        if ($p.PSObject.Properties.Name -notcontains 'Prefered' -and
            $p.PSObject.Properties.Name -notcontains 'Preferred') {
            $isPreferredRow = $true
        }
    }

    if (-not $isPreferredRow) { continue }

    if ($p.id) {
        [void]$preferredIdSet.Add($p.id.ToString())
    }
    if ($p.name) {
        [void]$preferredNameSet.Add((Normalize-Name -Value $p.name))
    }
}

Write-Log -Level INFO -Message ("Preferred IDs: {0}; Preferred names: {1}" -f $preferredIdSet.Count, $preferredNameSet.Count)

# ----------------------------------------
# 5. Matching helper: find IPTV channel
# ----------------------------------------

function Find-IptvMatch {
    param(
        [PSCustomObject]$BaselineRow
    )

    # Try by xmltv_id -> IPTV id
    $xmltvId = $null
    if ($BaselineRow.PSObject.Properties.Name -contains 'xmltv_id') {
        $xmltvId = $BaselineRow.xmltv_id
    }

    if ($xmltvId) {
        $idKey = $xmltvId.ToString().ToLowerInvariant()
        if ($iptvById.ContainsKey($idKey)) {
            return $iptvById[$idKey]
        }
    }

    # Try by normalized name and country
    $nameVal = $null
    if ($BaselineRow.PSObject.Properties.Name -contains 'name') {
        $nameVal = $BaselineRow.name
    }
    $countryVal = $null
    if ($BaselineRow.PSObject.Properties.Name -contains 'country') {
        $countryVal = $BaselineRow.country
    }

    $normName = Normalize-Name -Value $nameVal
    $countryKey = ($countryVal ? $countryVal.Trim() : "")

    $key = "{0}|{1}" -f $normName, $countryKey

    if ($iptvByNameCountry.ContainsKey($key)) {
        $candidates = $iptvByNameCountry[$key]
        if ($candidates.Count -gt 1) {
            Write-Log -Level WARN -Message ("Multiple IPTV matches for baseline '{0}' ({1}) – picking first" -f $nameVal, $countryKey)
        }
        return $candidates[0]
    }

    # No match
    return $null
}

# ----------------------------------------
# 6. Enrich baseline rows
# ----------------------------------------

$Enriched = @()
$baselineCount = 0
$matchedCount  = 0

foreach ($row in $Baseline) {
    if (-not $row) { continue }
    $baselineCount++

    $iptv = Find-IptvMatch -BaselineRow $row

    $dbId         = $null
    $dbName       = $null
    $dbCountry    = $null
    $dbCategories = $null
    $dbIsNsfw     = $null
    $dbNetwork    = $null
    $dbWebsite    = $null

    if ($iptv) {
        $matchedCount++
        $dbId         = $iptv.id
        $dbName       = $iptv.name
        $dbCountry    = $iptv.country
        $dbCategories = $iptv.categories
        $dbIsNsfw     = $iptv.is_nsfw
        $dbNetwork    = $iptv.network
        $dbWebsite    = $iptv.website
    }

    # Determine preferred / excluded flags
    $isPreferred = $false
    if ($dbId -and $preferredIdSet.Contains($dbId)) {
        $isPreferred = $true
    } elseif ($dbName) {
        $normDbName = Normalize-Name -Value $dbName
        if ($preferredNameSet.Contains($normDbName)) {
            $isPreferred = $true
        }
    }

    $excludedByCategory = $false
    if ($dbCategories -and $ExcludedCategorySet.Count -gt 0) {
        $cats = $dbCategories -split ';'
        foreach ($c in $cats) {
            $trimCat = $c.Trim()
            if ($trimCat -and $ExcludedCategorySet.Contains($trimCat)) {
                $excludedByCategory = $true
                break
            }
        }
    }

    $effectiveInclude = $false
    if ($isPreferred -and -not $excludedByCategory) {
        $effectiveInclude = $true
    }

    # Build enriched row – preserve all baseline fields, add db_* and flags
    $enrichedRow = [ordered]@{}

    foreach ($prop in $row.PSObject.Properties) {
        $enrichedRow[$prop.Name] = $prop.Value
    }

    $enrichedRow['db_id']         = $dbId
    $enrichedRow['db_name']       = $dbName
    $enrichedRow['db_country']    = $dbCountry
    $enrichedRow['db_categories'] = $dbCategories
    $enrichedRow['db_is_nsfw']    = $dbIsNsfw
    $enrichedRow['db_network']    = $dbNetwork
    $enrichedRow['db_website']    = $dbWebsite

    $enrichedRow['IsPreferred']        = $isPreferred
    $enrichedRow['ExcludedByCategory'] = $excludedByCategory
    $enrichedRow['EffectiveInclude']   = $effectiveInclude

    $Enriched += New-Object PSObject -Property $enrichedRow
}

Write-Log -Level INFO -Message ("Baseline rows processed: {0}" -f $baselineCount)
Write-Log -Level INFO -Message ("Baseline rows matched to IPTV-org: {0}" -f $matchedCount)

# ----------------------------------------
# 7. Export enriched CSVs
# ----------------------------------------

if (($Enriched -eq $null) -or ($Enriched.Count -eq 0)) {
    Write-Log -Level WARN -Message "No enriched rows to export. This might be expected if baseline was empty."
} else {
    try {
        $Enriched | Export-Csv -NoTypeInformation -Path $EnrichedStableCsv -Encoding UTF8
        Write-Log -Level OK -Message ("Wrote stable enriched CSV: {0}" -f $EnrichedStableCsv)
    }
    catch {
        Write-Log -Level ERROR -Message ("Failed to write stable enriched CSV: {0}" -f $_.Exception.Message)
        if ($Mode -eq "Strict") {
            throw
        }
    }

    try {
        $Enriched | Export-Csv -NoTypeInformation -Path $EnrichedVersioned -Encoding UTF8
        Write-Log -Level OK -Message ("Wrote versioned enriched CSV: {0}" -f $EnrichedVersioned)
    }
    catch {
        Write-Log -Level ERROR -Message ("Failed to write versioned enriched CSV: {0}" -f $_.Exception.Message)
        if ($Mode -eq "Strict") {
            throw
        }
    }
}

Write-Log -Level INFO -Message ("Finished {0} v{1}" -f $ScriptName, $ScriptVersion)
