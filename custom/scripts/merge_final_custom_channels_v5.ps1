<#
Script: merge_final_custom_channels_v5.ps1
Purpose:
  Merge all your channel sources into ONE final custom.channels.xml
  - Style A: keep provider/broadcaster naming exactly (no cleanup)
  - UK-A2: keep only ONE primary for each UK network (collapse regionals)
  - AU: keep only one primary for each AU network (collapse city variants)
  - CA/US locals preserved for your explicit keep rules
  - Prefers xmltv_id, then site priority
  - Option 1 best-practice: NEVER halt on bad metadata; log & continue
Inputs (local only):
  custom/output/custom.channels.deduped.v2.xml
  custom/output/custom.channels.xml
  custom/output/Draft-Keep.csv
  custom/output/matched_channels.csv
  custom/output/recommended_custom_list.csv
  custom/output/consolidated_inventory.csv (optional Draft-Keep lookup)

Outputs:
  custom/output/custom.channels.final.v5.xml
  custom/output/merge_final_report.v5.csv
  custom/output/problem_channels.v5.csv

Logs:
  custom/logs/merge_final_custom_channels_v5.log
#>

param()

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$CustomPath = Join-Path $BasePath "custom"
$OutputPath = Join-Path $CustomPath "output"
$LogPath    = Join-Path $CustomPath "logs"
$ScriptName = "merge_final_custom_channels_v5.ps1"
$MainLog    = Join-Path $LogPath "merge_final_custom_channels_v5.log"

# Inputs
$InDedupedXml = Join-Path $OutputPath "custom.channels.deduped.v2.xml"
$InCustomXml  = Join-Path $OutputPath "custom.channels.xml"
$InDraftKeep  = Join-Path $OutputPath "Draft-Keep.csv"
$InMatched    = Join-Path $OutputPath "matched_channels.csv"
$InRecom      = Join-Path $OutputPath "recommended_custom_list.csv"
$InInventory  = Join-Path $OutputPath "consolidated_inventory.csv"  # optional Draft-Keep lookup

# Outputs
$OutFinalXml   = Join-Path $OutputPath "custom.channels.final.v5.xml"
$OutReportCsv  = Join-Path $OutputPath "merge_final_report.v5.csv"
$OutProblemCsv = Join-Path $OutputPath "problem_channels.v5.csv"

foreach ($f in @($CustomPath,$OutputPath,$LogPath)) {
  if (!(Test-Path $f)) { New-Item -ItemType Directory -Force -Path $f | Out-Null }
}

function Write-Log {
  param(
    [string]$Message,
    [string]$Level="INFO"
  )
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "[$ts][$Level] $Message" | Tee-Object -FilePath $MainLog -Append
}

function Perf-Start { return [System.Diagnostics.Stopwatch]::StartNew() }
function Perf-Stop([System.Diagnostics.Stopwatch]$sw, [string]$label) {
  $sw.Stop()
  Write-Log "[PERF] $label in $([math]::Round($sw.Elapsed.TotalSeconds,2))s"
}

Write-Log "Starting $ScriptName"
Write-Log "BasePath=$BasePath"
Write-Log "OutputPath=$OutputPath"

# ---------------------------------------------------------------------------
# SITE PRIORITY (winner when duplicates)
# ---------------------------------------------------------------------------
$SitePriority = @{
  "epgshare01.online"           = 1
  "directv.com"                 = 2
  "tv24.co.uk"                  = 3
  "abc.net.au"                  = 4
  "streamingtvguides.com"       = 5
  "tvinsider.com"               = 6
  "tvguide.com"                 = 7
  "pluto.tv"                    = 8
  "plex.tv"                     = 9
  "virgintvgo.virginmedia.com"  = 10
  "player.ee.co.uk"             = 11
  "freeview.co.uk"              = 12
  "sky.com"                     = 13
}

# ---------------------------------------------------------------------------
# NORMALIZATION RULES
# ---------------------------------------------------------------------------
$QualityTokens = @("hd","uhd","4k","fhd","sd")

# UK/AU regional tokens to drop for grouping (UK-A2, AU single-primary)
$UkDropTokens = @(
  "london","scotland","wales","england","yorkshire","midlands","meridian","anglia",
  "central","granada","tyne","tees","border","westcountry","calendar","ulster"
)

$AuDropTokens = @(
  "sydney","melbourne","brisbane","adelaide","perth","hobart","darwin","canberra",
  "nsw","vic","qld","wa","sa","nt","tas"
)

# CA/US locals you WANT to keep distinct
$KeepLocalPatterns = @(
  # CTV
  "ctv.*kitchener","ctv.*london","ctv.*barrie","ctv.*toronto","ctv.*halifax","ctv.*vancouver",
  # CBC
  "cbc.*toronto","cbc.*halifax","cbc.*vancouver",
  # US Buffalo + East/West
  "buffalo","niagara","wned","wkbw","wivb","wutv","wgrz",
  "abc.*east","abc.*west","cbs.*east","cbs.*west","nbc.*east","nbc.*west","fox.*east","fox.*west","pbs.*east","pbs.*west"
)

function Normalize-Name([string]$name) {
  if ([string]::IsNullOrWhiteSpace($name)) { return "" }
  $n = $name.ToLower()

  foreach ($q in $QualityTokens) {
    $n = $n -replace "\b$q\b"," "
  }

  $n = $n -replace "[^a-z0-9\+ ]"," "
  $n = ($n -replace "\s+"," ").Trim()
  return $n
}

function Is-LocalKeep([string]$norm) {
  foreach ($p in $KeepLocalPatterns) {
    if ($norm -match $p) { return $true }
  }
  return $false
}

function Collapse-UkKey([string]$norm) {
  $k = $norm
  foreach ($t in $UkDropTokens) {
    $k = $k -replace "\b$t\b"," "
  }
  $k = ($k -replace "\s+"," ").Trim()
  return $k
}

function Collapse-AuKey([string]$norm) {
  $k = $norm
  foreach ($t in $AuDropTokens) {
    $k = $k -replace "\b$t\b"," "
  }
  $k = ($k -replace "\s+"," ").Trim()
  return $k
}

# Guess country safely (never crashes on null site)
function Guess-Country([pscustomobject]$ch) {
  $site = ""
  if ($ch.site) { $site = $ch.site.ToLower() }

  $name = Normalize-Name $ch.name

  if ($site -match "abc\.net\.au" -or $name -match "\b(australia|sbs|seven|nine|ten)\b") { return "AU" }
  if ($site -match "tv24\.co\.uk|freeview\.co\.uk|sky\.com|virginmedia|ee\.co\.uk" -or $name -match "\b(bbc|itv|channel 4|channel 5|sky)\b") { return "UK" }
  if ($site -match "directv\.com|tvinsider\.com|tvguide\.com|pluto\.tv|plex\.tv|stream" -or $name -match "\b(usa|us)\b") { return "US" }
  if ($name -match "\b(canada|cbc|ctv|global|citytv|tva)\b") { return "CA" }

  return "NA"
}

# ---------------------------------------------------------------------------
# DIAGNOSTIC COLLECTION
# ---------------------------------------------------------------------------
$ProblemRows = New-Object System.Collections.Generic.List[object]

function Add-ProblemRow([pscustomobject]$ch, [string]$problem, [string]$normKey) {
  $ProblemRows.Add([pscustomobject]@{
    name            = $ch.name
    site            = $ch.site
    xmltv_id        = $ch.xmltv_id
    site_id         = $ch.site_id
    source          = $ch.source
    problem         = $problem
    normalized_key  = $normKey
  }) | Out-Null
}

# ---------------------------------------------------------------------------
# LOAD HELPERS
# ---------------------------------------------------------------------------
function Load-ChannelsXml($path, $sourceTag) {
  $list = @()
  if (!(Test-Path $path)) {
    Write-Log "Missing XML: $path" "WARN"
    return $list
  }
  [xml]$x = Get-Content $path
  foreach ($c in $x.channels.channel) {
    $nameText = "$($c.'#text')".Trim()
    if ([string]::IsNullOrWhiteSpace($nameText)) {
      Write-Log "[MISSING_NAME] source=$sourceTag site=$($c.site) xmltv_id=$($c.xmltv_id)" "WARN"
      continue
    }
    $list += [pscustomobject]@{
      source   = $sourceTag
      site     = $c.site
      xmltv_id = $c.xmltv_id
      site_id  = $c.site_id
      name     = $nameText
    }
  }
  Write-Log "Loaded XML $sourceTag count=$($list.Count)"
  return $list
}

function Load-Csv($path, $sourceTag) {
  $list = @()
  if (!(Test-Path $path)) {
    Write-Log "Missing CSV: $path" "WARN"
    return $list
  }
  $rows = Import-Csv $path
  foreach ($r in $rows) {
    $name = $r.Channel
    if (-not $name) { $name = $r."Channel Name" }
    if (-not $name) { $name = $r.name }
    if ([string]::IsNullOrWhiteSpace($name)) {
      Write-Log "[MISSING_NAME] source=$sourceTag raw_row=$(($r | ConvertTo-Json -Compress))" "WARN"
      continue
    }

    $list += [pscustomobject]@{
      source   = $sourceTag
      site     = $r.site
      xmltv_id = $r.xmltv_id
      site_id  = $r.site_id
      name     = "$name".Trim()
    }
  }
  Write-Log "Loaded CSV $sourceTag count=$($list.Count)"
  return $list
}

# Inventory for Draft-Keep lookup (optional)
$Inventory = @()
$swInv = Perf-Start
if (Test-Path $InInventory) {
  $Inventory = Import-Csv $InInventory
  Write-Log "Loaded consolidated_inventory.csv rows=$($Inventory.Count)"
} else {
  Write-Log "Missing consolidated_inventory.csv (Draft-Keep matches will be name-only)" "WARN"
}
Perf-Stop $swInv "Inventory load"

function Lookup-InventoryMatch($draftName) {
  if ($Inventory.Count -eq 0) { return $null }
  $dn = Normalize-Name $draftName
  if (-not $dn) { return $null }

  $hit = $Inventory | Where-Object {
    (Normalize-Name $_.name) -eq $dn
  } | Select-Object -First 1

  if ($hit) {
    return [pscustomobject]@{
      source   = "DraftKeep->Inventory"
      site     = $hit.site
      xmltv_id = $hit.xmltv_id
      site_id  = $hit.site_id
      name     = $hit.name
    }
  }
  return $null
}

# ---------------------------------------------------------------------------
# BUILD CANDIDATES
# ---------------------------------------------------------------------------
$swCand = Perf-Start

$candidates = @()
$candidates += Load-ChannelsXml $InDedupedXml "deduped.v2"
$candidates += Load-ChannelsXml $InCustomXml  "custom.base"
$candidates += Load-Csv         $InMatched    "matched"
$candidates += Load-Csv         $InRecom      "recommended"

$draftRows = Load-Csv $InDraftKeep "draftkeep"
foreach ($d in $draftRows) {
  $m = Lookup-InventoryMatch $d.name
  if ($m) { $candidates += $m }
  else    { $candidates += $d }
}

Perf-Stop $swCand "Candidate build"
Write-Log "Total candidates before merge=$($candidates.Count)"

# ---------------------------------------------------------------------------
# MERGE / DEDUPE (Style A + UK-A2)
# ---------------------------------------------------------------------------
$swMerge = Perf-Start
$kept = @{}
$report = @()

foreach ($ch in $candidates) {

  # Diagnostics for missing metadata
  if (-not $ch.site) {
    Write-Log "[MISSING_SITE] source=$($ch.source) name=""$($ch.name)"" xmltv_id=""$($ch.xmltv_id)""" "WARN"
    Add-ProblemRow $ch "missing_site" ""
  }
  if (-not $ch.xmltv_id) {
    Write-Log "[MISSING_XMLTV] source=$($ch.source) name=""$($ch.name)"" site=""$($ch.site)""" "WARN"
    Add-ProblemRow $ch "missing_xmltv_id" ""
  }

  $norm = Normalize-Name $ch.name
  if (-not $norm) {
    Write-Log "[BAD_NORMALIZATION] source=$($ch.source) name=""$($ch.name)""" "WARN"
    Add-ProblemRow $ch "bad_normalization" ""
    continue
  }

  $country   = Guess-Country $ch
  $localKeep = Is-LocalKeep $norm

  # base key rules:
  # - CA/US locals stay distinct
  # - UK collapse regionals
  # - AU collapse city variants
  $key = $norm

  if (-not $localKeep) {
    if ($country -eq "UK") { $key = Collapse-UkKey $key }
    elseif ($country -eq "AU") { $key = Collapse-AuKey $key }

    $key = $key -replace "\b(east|west|\+1|\+2|timeshift)\b"," "
    $key = ($key -replace "\s+"," ").Trim()
  }

  # If xmltv_id exists, include it in key to avoid over-collapse between truly different channels
  if ($ch.xmltv_id) {
    $key = "$key|$($ch.xmltv_id)"
  }

  Write-Log "[TRACE][KEY] name=""$($ch.name)"" -> key=""$key"" country=$country local_keep=$localKeep" "DEBUG"

  if ($kept.ContainsKey($key)) {
    $prev = $kept[$key]

    $pPrev = $SitePriority[$prev.site]; if (-not $pPrev) { $pPrev = 99 }
    $pNew  = $SitePriority[$ch.site];   if (-not $pNew)  { $pNew  = 99 }

    $newWins = $false

    # prefer having xmltv_id
    if ([string]::IsNullOrWhiteSpace($prev.xmltv_id) -and -not [string]::IsNullOrWhiteSpace($ch.xmltv_id)) {
      $newWins = $true
      Write-Log "[TRACE][WIN] xmltv_id preference: new wins for key=$key" "DEBUG"
    }
    # else prefer higher-priority site
    elseif ($pNew -lt $pPrev) {
      $newWins = $true
      Write-Log "[TRACE][WIN] site priority ${pNew}<${pPrev}: new wins for key=$key" "DEBUG"
    }

    if ($newWins) {
      $kept[$key] = $ch
    }

    $report += [pscustomobject]@{
      key              = $key
      kept_name        = $kept[$key].name
      kept_site        = $kept[$key].site
      kept_xmltv_id    = $kept[$key].xmltv_id
      dropped_name     = $ch.name
      dropped_site     = $ch.site
      dropped_xmltv_id = $ch.xmltv_id
      decision_source  = $kept[$key].source
      dropped_source   = $ch.source
      country_rule     = $country
      local_keep       = $localKeep
    }
  } else {
    $kept[$key] = $ch
  }
}

$final = $kept.Values
Perf-Stop $swMerge "Merge/dedupe"
Write-Log "Final unique channels=$($final.Count)"
Write-Log "Duplicates collapsed=$($report.Count)"
Write-Log "Problems recorded=$($ProblemRows.Count)"

# ---------------------------------------------------------------------------
# WRITE FINAL XML (Style A: keep original names)
# ---------------------------------------------------------------------------
$swWriteXml = Perf-Start

$doc = New-Object System.Xml.XmlDocument
$decl = $doc.CreateXmlDeclaration("1.0","utf-8",$null)
$doc.AppendChild($decl) | Out-Null

$root = $doc.CreateElement("channels")
$doc.AppendChild($root) | Out-Null

foreach ($ch in $final | Sort-Object site,name) {
  $node = $doc.CreateElement("channel")
  if ($ch.site)     { $node.SetAttribute("site",$ch.site) }
  if ($ch.xmltv_id) { $node.SetAttribute("xmltv_id",$ch.xmltv_id) }
  if ($ch.site_id)  { $node.SetAttribute("site_id",$ch.site_id) }

  $node.InnerText = $ch.name
  $root.AppendChild($node) | Out-Null
}

$doc.Save($OutFinalXml)
Perf-Stop $swWriteXml "Write final XML"
Write-Log "Wrote final XML: $OutFinalXml"

# ---------------------------------------------------------------------------
# WRITE REPORT + PROBLEM CSV
# ---------------------------------------------------------------------------
$swWriteCsv = Perf-Start

$report | Export-Csv -NoTypeInformation -Encoding UTF8 $OutReportCsv
Write-Log "Wrote merge report: $OutReportCsv"

if ($ProblemRows.Count -gt 0) {
  $ProblemRows | Export-Csv -NoTypeInformation -Encoding UTF8 $OutProblemCsv
  Write-Log "Wrote problem channels CSV: $OutProblemCsv" "WARN"
} else {
  Write-Log "No problem channels detected."
}

Perf-Stop $swWriteCsv "Write CSV outputs"
Write-Log "Done."

Write-Host "`nDONE! Final custom channel file created:`n$OutFinalXml`nReport:`n$OutReportCsv`nProblems:`n$OutProblemCsv`n"
