<#
Script: merge_final_custom_channels_v3.ps1
Purpose:
  Merge your deduped XML + Draft-Keep + matched + recommended into ONE final custom.channels.xml
  - Local only
  - Clean/simplified display names (style B)
  - Prefers entries with xmltv_id
  - Prefers higher-priority sites
Outputs:
  custom/output/custom.channels.final.v3.xml
  custom/output/merge_final_report.v3.csv
Logs:
  custom/logs/merge_final_custom_channels_v3.log
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
$ScriptName = "merge_final_custom_channels_v3.ps1"
$MainLog    = Join-Path $LogPath "merge_final_custom_channels_v3.log"

# Inputs
$InDedupedXml = Join-Path $OutputPath "custom.channels.deduped.v2.xml"
$InCustomXml  = Join-Path $OutputPath "custom.channels.xml"
$InDraftKeep  = Join-Path $OutputPath "Draft-Keep.csv"
$InMatched    = Join-Path $OutputPath "matched_channels.csv"
$InRecom      = Join-Path $OutputPath "recommended_custom_list.csv"
$InInventory  = Join-Path $OutputPath "consolidated_inventory.csv"  # used for Draft-Keep->xmltv lookup

# Outputs
$OutFinalXml  = Join-Path $OutputPath "custom.channels.final.v3.xml"
$OutReportCsv = Join-Path $OutputPath "merge_final_report.v3.csv"

foreach ($f in @($CustomPath,$OutputPath,$LogPath)) {
  if (!(Test-Path $f)) { New-Item -ItemType Directory -Force -Path $f | Out-Null }
}

function Write-Log {
  param([string]$Message)
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "[$ts] $Message" | Tee-Object -FilePath $MainLog -Append
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
}

# ---------------------------------------------------------------------------
# NORMALIZATION + SIMPLIFIED DISPLAY NAMES (Style B)
# ---------------------------------------------------------------------------
$QualityTokens = @("hd","uhd","4k","fhd","sd")
$AuUkDropTokens = @(
  "sydney","melbourne","brisbane","adelaide","perth","hobart","darwin","canberra",
  "london","central","yorkshire","scotland","wales","midlands","england",
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

  # strip quality tokens
  foreach ($q in $QualityTokens) {
    $n = $n -replace "\b$q\b"," "
  }

  # strip punctuation
  $n = $n -replace "[^a-z0-9\+ ]"," "

  # drop AU/UK city/region tokens
  foreach ($t in $AuUkDropTokens) {
    $n = $n -replace "\b$t\b"," "
  }

  # collapse spaces
  $n = ($n -replace "\s+"," ").Trim()
  return $n
}

function Is-LocalKeep([string]$norm) {
  foreach ($p in $KeepLocalPatterns) {
    if ($norm -match $p) { return $true }
  }
  return $false
}

function Simplify-Display([string]$name) {
  # Style B: clean display text
  $n = Normalize-Name $name

  # remove generic timeshift for non-local channels
  if (-not (Is-LocalKeep $n)) {
    $n = $n -replace "\b(\+1|\+2|timeshift)\b"," "
    $n = ($n -replace "\s+"," ").Trim()
  }

  # Title Case-ish
  $words = $n.Split(" ") | Where-Object { $_ -ne "" } | ForEach-Object {
    $_.Substring(0,1).ToUpper() + $_.Substring(1)
  }
  return ($words -join " ")
}

# ---------------------------------------------------------------------------
# LOAD HELPERS
# ---------------------------------------------------------------------------
function Load-ChannelsXml($path, $sourceTag) {
  $list = @()
  if (!(Test-Path $path)) {
    Write-Log "WARN missing XML: $path"
    return $list
  }
  [xml]$x = Get-Content $path
  foreach ($c in $x.channels.channel) {
    $list += [pscustomobject]@{
      source   = $sourceTag
      site     = $c.site
      xmltv_id = $c.xmltv_id
      site_id  = $c.site_id
      name     = "$($c.'#text')".Trim()
    }
  }
  Write-Log "Loaded XML $sourceTag count=$($list.Count)"
  return $list
}

function Load-Csv($path, $sourceTag) {
  $list = @()
  if (!(Test-Path $path)) {
    Write-Log "WARN missing CSV: $path"
    return $list
  }
  $rows = Import-Csv $path
  foreach ($r in $rows) {
    # try common columns
    $name = $r.Channel
    if (-not $name) { $name = $r."Channel Name" }
    if (-not $name) { $name = $r.name }
    if (-not $name) { continue }

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

# Inventory for Draft-Keep lookup
$Inventory = @()
if (Test-Path $InInventory) {
  $Inventory = Import-Csv $InInventory
  Write-Log "Loaded consolidated_inventory.csv rows=$($Inventory.Count)"
} else {
  Write-Log "WARN missing consolidated_inventory.csv (Draft-Keep matches will be name-only)"
}

function Lookup-InventoryMatch($draftName) {
  if ($Inventory.Count -eq 0) { return $null }

  $dn = Normalize-Name $draftName
  if (-not $dn) { return $null }

  # find best exact normalized hit first
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
$candidates = @()

# Priority: your deduped list first
$candidates += Load-ChannelsXml $InDedupedXml "deduped.v2"

# Then any remaining from your current custom
$candidates += Load-ChannelsXml $InCustomXml "custom.base"

# Matched / recommended lists (already mapped)
$candidates += Load-Csv $InMatched "matched"
$candidates += Load-Csv $InRecom "recommended"

# Draft-Keep: resolve against inventory when possible
$draftRows = Load-Csv $InDraftKeep "draftkeep"
foreach ($d in $draftRows) {
  $m = Lookup-InventoryMatch $d.name
  if ($m) { $candidates += $m }
  else { $candidates += $d }
}

Write-Log "Total candidates before merge=$($candidates.Count)"

# ---------------------------------------------------------------------------
# MERGE / DEDUPE
# ---------------------------------------------------------------------------
$kept = @{}
$report = @()

foreach ($ch in $candidates) {

  $norm = Normalize-Name $ch.name
  if (-not $norm) { continue }

  $localKeep = Is-LocalKeep $norm

  # key: localKeep keeps full norm; otherwise strip east/west/+1
  $key = $norm
  if (-not $localKeep) {
    $key = $key -replace "\b(east|west|\+1|\+2)\b"," "
    $key = ($key -replace "\s+"," ").Trim()
  }

  if ($kept.ContainsKey($key)) {
    $prev = $kept[$key]

    $pPrev = $SitePriority[$prev.site]; if (-not $pPrev) { $pPrev = 99 }
    $pNew  = $SitePriority[$ch.site];   if (-not $pNew)  { $pNew  = 99 }

    $newWins = $false

    # prefer having xmltv_id
    if ([string]::IsNullOrWhiteSpace($prev.xmltv_id) -and -not [string]::IsNullOrWhiteSpace($ch.xmltv_id)) {
      $newWins = $true
    }
    # then prefer higher-priority site
    elseif ($pNew -lt $pPrev) {
      $newWins = $true
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
    }
  } else {
    $kept[$key] = $ch
  }
}

$final = $kept.Values
Write-Log "Final unique channels=$($final.Count)"
Write-Log "Duplicates collapsed=$($report.Count)"

# ---------------------------------------------------------------------------
# WRITE FINAL XML (simplified display names)
# ---------------------------------------------------------------------------
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

  $node.InnerText = (Simplify-Display $ch.name)

  $root.AppendChild($node) | Out-Null
}

$doc.Save($OutFinalXml)
Write-Log "Wrote final XML: $OutFinalXml"

# ---------------------------------------------------------------------------
# WRITE REPORT
# ---------------------------------------------------------------------------
$report | Export-Csv -NoTypeInformation -Encoding UTF8 $OutReportCsv
Write-Log "Wrote merge report: $OutReportCsv"
Write-Log "Done."
Write-Host "`nDONE! Final custom channel file created:`n$OutFinalXml`nReport:`n$OutReportCsv`n"
