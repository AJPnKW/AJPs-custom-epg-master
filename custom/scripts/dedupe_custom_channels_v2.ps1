<#
Script: dedupe_custom_channels_v2.ps1
Purpose: Deduplicate custom.channels.merged.xml using country rules + site priority
Author: ChatGPT for Andrew
Created: 2025-11-22
Version: 2.0

Input:
  custom/output/custom.channels.merged.xml

Outputs:
  custom/output/custom.channels.deduped.v2.xml
  custom/output/dedupe_report.v2.csv
  custom/logs/dedupe_custom_channels_v2.log
#>

param()

$ErrorActionPreference = "Stop"

# ---------- Paths ----------
$BasePath  = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$InPath    = Join-Path $BasePath "custom\output\custom.channels.merged.xml"
$OutXml    = Join-Path $BasePath "custom\output\custom.channels.deduped.v2.xml"
$OutReport = Join-Path $BasePath "custom\output\dedupe_report.v2.csv"
$LogPath   = Join-Path $BasePath "custom\logs\dedupe_custom_channels_v2.log"

New-Item -ItemType Directory -Force -Path (Split-Path $OutXml) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $LogPath) | Out-Null

function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "[$ts] $msg"
  $line | Tee-Object -FilePath $LogPath -Append
}

Log "Starting dedupe_custom_channels_v2.ps1"
Log "Input=$InPath"
Log "Output=$OutXml"

# ---------- Site priority ----------
$SitePriority = @{
  "epgshare01.online"           = 1
  "directv.com"                 = 2
  "tv24.co.uk"                  = 3
  "abc.net.au"                  = 4
  "streamingtvguides.com"       = 5
  "tvinsider.com"               = 6
  "pluto.tv"                    = 7
  "virgintvgo.virginmedia.com"  = 8
}

# ---------- Country rules ----------
# AU/UK collapse city + region variants
$AuUkDropTokens = @(
  "sydney","melbourne","brisbane","adelaide","perth","hobart","darwin","canberra",
  "london","central","yorkshire","scotland","wales","midlands","plus","hour"
)

# CA/US allowlist for locals/time-shift you specifically want
$KeepLocalPatterns = @(
  # CTV
  "ctv.*kitchener","ctv.*london","ctv.*barrie","ctv.*toronto","ctv.*halifax","ctv.*vancouver",
  # CBC
  "cbc.*toronto","cbc.*halifax","cbc.*vancouver",
  # US Buffalo + East/West
  "buffalo","niagara","wned","wkbw","wivb","wutv","wgrz",
  "abc.*east","abc.*west","cbs.*east","cbs.*west","nbc.*east","nbc.*west","fox.*east","fox.*west","pbs.*east","pbs.*west"
)

function Normalize-Name($name, $site) {
  if ([string]::IsNullOrWhiteSpace($name)) { return "" }
  $n = $name.ToLower()

  # strip quality tags
  $n = $n -replace "\b(hd|uhd|4k|fhd|sd)\b"," "
  # strip punctuation
  $n = $n -replace "[^a-z0-9\+ ]"," "
  # collapse spaces
  $n = ($n -replace "\s+"," ").Trim()

  # AU/UK city-collapse: remove tokens if *not* in local allowlist
  foreach ($t in $AuUkDropTokens) {
    $n = $n -replace "\b$t\b"," "
  }
  $n = ($n -replace "\s+"," ").Trim()

  return $n
}

# ---------- Load XML ----------
[xml]$xml = Get-Content $InPath
$channels = @()

foreach ($c in $xml.channels.channel) {
  $channels += [pscustomobject]@{
    site     = $c.site
    xmltv_id = $c.xmltv_id
    site_id  = $c.site_id
    name     = "$($c.'#text')".Trim()
  }
}

Log "Loaded channels=$($channels.Count)"

# ---------- Deduplicate ----------
$kept = @{}
$report = @()

foreach ($ch in $channels) {

  $norm = Normalize-Name $ch.name $ch.site
  if (-not $norm) { continue }

  # local keep check
  $isLocalKeep = $false
  foreach ($p in $KeepLocalPatterns) {
    if ($norm -match $p) { $isLocalKeep = $true; break }
  }

  # key: if local keep -> include full norm with city; else strip +1/+2/east/west for grouping
  $key = $norm
  if (-not $isLocalKeep) {
    $key = $key -replace "\b(\+1|\+2|east|west)\b"," "
    $key = ($key -replace "\s+"," ").Trim()
  }

  if ($kept.ContainsKey($key)) {
    $prev = $kept[$key]

    $pPrev = $SitePriority[$prev.site]
    $pNew  = $SitePriority[$ch.site]
    if (-not $pPrev) { $pPrev = 99 }
    if (-not $pNew)  { $pNew  = 99 }

    # prefer xmltv_id then site priority
    $newWins = $false
    if ([string]::IsNullOrWhiteSpace($prev.xmltv_id) -and -not [string]::IsNullOrWhiteSpace($ch.xmltv_id)) {
      $newWins = $true
    } elseif ($pNew -lt $pPrev) {
      $newWins = $true
    }

    if ($newWins) {
      $kept[$key] = $ch
    }

    $report += [pscustomobject]@{
      key=$key; kept_name=$kept[$key].name; kept_site=$kept[$key].site; kept_xmltv_id=$kept[$key].xmltv_id
      dropped_name=$ch.name; dropped_site=$ch.site; dropped_xmltv_id=$ch.xmltv_id
    }

  } else {
    $kept[$key] = $ch
  }
}

$final = $kept.Values
Log "Final unique channels=$($final.Count)"
Log "Duplicates collapsed=$($report.Count)"

# ---------- Write XML ----------
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

$doc.Save($OutXml)
Log "Wrote $OutXml"

# ---------- Write report CSV ----------
$report | Export-Csv -NoTypeInformation -Encoding UTF8 $OutReport
Log "Wrote $OutReport"
Log "Done."
