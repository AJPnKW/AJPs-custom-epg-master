<#
Script Name: extract_scoped_local_channels_only.ps1
Purpose:
  FAST local-only channel inventory builder.
  - NO npm / NO grab / NO internet
  - Reads ONLY the scoped *.channels.xml files you care about.
Outputs:
  custom/output/consolidated_inventory.csv
  custom/output/consolidated_channels_only.xml
Logs:
  custom/logs/extract_scoped_local_channels_only.log
#>

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$SitesPath  = Join-Path $BasePath "sites"

$CustomPath = Join-Path $BasePath "custom"
$OutputPath = Join-Path $CustomPath "output"
$LogPath    = Join-Path $CustomPath "logs"

$ScriptName = "extract_scoped_local_channels_only.ps1"
$MainLog    = Join-Path $LogPath "extract_scoped_local_channels_only.log"

# Ensure folders exist
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
Write-Log "SitesPath=$SitesPath"
Write-Log "OutputPath=$OutputPath"

# ---------------------------------------------------------------------------
# YOUR SCOPED SITE FOLDERS (TOP-LEVEL *.channels.xml ONLY)
# ---------------------------------------------------------------------------
$ScopedSiteFolders = @(
  "abc.net.au",
  "directv.com",
  "virgintvgo.virginmedia.com",
  "tvinsider.com",
  "tvguide.com",
  "tv24.co.uk",
  "streamingtvguides.com",
  "pluto.tv",
  "plex.tv",
  "player.ee.co.uk"
)

# ---------------------------------------------------------------------------
# YOUR EPGSHARE SCOPED FILES ONLY
# ---------------------------------------------------------------------------
$EpgShareFolder = "epgshare01.online"
$EpgShareFiles  = @(
  "epgshare01.online_AU1.channels.xml",
  "epgshare01.online_CA1.channels.xml",
  "epgshare01.online_US1.channels.xml",
  "epgshare01.online_US_LOCALS2.channels.xml",
  "epgshare01.online_UK1.channels.xml"
)

# ---------------------------------------------------------------------------
# BUILD EXPLICIT FILE LIST (NO RECURSION, NO EXTRA SITES)
# ---------------------------------------------------------------------------
$FilesToProcess = @()

# 1) Add all top-level *.channels.xml in each scoped folder
foreach ($folderName in $ScopedSiteFolders) {
  $folderPath = Join-Path $SitesPath $folderName
  if (Test-Path $folderPath) {
    $files = Get-ChildItem -Path $folderPath -Filter "*.channels.xml" -File -ErrorAction SilentlyContinue
    foreach ($f in $files) { $FilesToProcess += $f.FullName }
    Write-Log "Scoped folder: $folderName → $($files.Count) file(s)"
  } else {
    Write-Log "WARN: Missing folder: $folderName"
  }
}

# 2) Add ONLY your 5 epgshare files
$epgPath = Join-Path $SitesPath $EpgShareFolder
if (Test-Path $epgPath) {
  foreach ($fname in $EpgShareFiles) {
    $full = Join-Path $epgPath $fname
    if (Test-Path $full) {
      $FilesToProcess += $full
      Write-Log "Added epgshare file: $fname"
    } else {
      Write-Log "WARN: Missing epgshare file: $fname"
    }
  }
} else {
  Write-Log "WARN: Missing epgshare folder: $EpgShareFolder"
}

$FilesToProcess = $FilesToProcess | Sort-Object -Unique
Write-Log "Total files to process: $($FilesToProcess.Count)"

if ($FilesToProcess.Count -eq 0) {
  throw "No channel files found. Check your sites folder and scope."
}

# ---------------------------------------------------------------------------
# EXTRACT INTO MASTER INVENTORY + CHANNELS-ONLY XML
# ---------------------------------------------------------------------------
$inventoryRows = @()

$xmlMaster = New-Object System.Xml.XmlDocument
$root = $xmlMaster.CreateElement("tv")
$xmlMaster.AppendChild($root) | Out-Null

foreach ($file in $FilesToProcess) {
  try {
    [xml]$x = Get-Content $file

    # Tag = parent folder name (site)
    $tag = Split-Path (Split-Path $file -Parent) -Leaf

    foreach ($c in $x.channels.channel) {

      $inventoryRows += [pscustomobject]@{
        tag         = $tag
        site        = $c.site
        name        = $c.'#text'
        xmltv_id    = $c.xmltv_id
        lang        = $c.lang
        site_id     = $c.site_id
        source_file = (Split-Path $file -Leaf)
      }

      # Consolidated <channel> node
      $chn = $xmlMaster.CreateElement("channel")
      $chn.SetAttribute("id", $c.xmltv_id)

      $dn = $xmlMaster.CreateElement("display-name")
      $dn.InnerText = $c.'#text'

      $chn.AppendChild($dn) | Out-Null
      $root.AppendChild($chn) | Out-Null
    }

    Write-Log "Processed: $file"
  }
  catch {
    Write-Log "ERROR reading $file : $($_.Exception.Message)"
  }
}

# ---------------------------------------------------------------------------
# WRITE OUTPUTS
# ---------------------------------------------------------------------------
$consolidatedCsv = Join-Path $OutputPath "consolidated_inventory.csv"
$consolidatedXml = Join-Path $OutputPath "consolidated_channels_only.xml"

$inventoryRows |
  Sort-Object tag,name |
  Export-Csv $consolidatedCsv -NoTypeInformation -Encoding UTF8

$xmlMaster.Save($consolidatedXml)

Write-Log "Wrote consolidated CSV: $consolidatedCsv"
Write-Log "Wrote consolidated XML: $consolidatedXml"
Write-Log "Completed $ScriptName"

Write-Host "`nDONE! Local scoped channel inventory built:`n$consolidatedCsv`n$consolidatedXml`n"
