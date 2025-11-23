<#
Script Name: discover_and_extract_all.ps1
Purpose:
  1) Run CHANNEL-ONLY discovery grabs for a list of CA/US/UK/AU candidate sites (PARALLEL)
  2) Extract channel inventories from each discover_*.xml into CSV (inside each job)
  3) Consolidate all discover XMLs (channels only) → consolidated_discover_channels_only.xml
  4) Consolidate all inventories → consolidated_inventory.csv
Author: Andrew J. Pearen (personal EPG project)
Repo Root:
  C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master
Notes:
  - Uses Start-Job parallelism for compatibility with Windows PowerShell 5.1 and PowerShell 7+
  - Uses resolved full path to npx to avoid PATH issues inside jobs
  - Forces channels-only mode with --days=0
  - Safe to re-run; overwrites discover/inventory outputs per site
#>

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$CustomPath = Join-Path $BasePath "custom"
$OutputPath = Join-Path $CustomPath "output"
$LogPath    = Join-Path $CustomPath "logs"
$DataPath   = Join-Path $CustomPath "data"
$CachePath  = Join-Path $CustomPath "cache"
$ScriptName = "discover_and_extract_all.ps1"

$MainLog    = Join-Path $LogPath "discover_and_extract_all.log"

# ---------------------------------------------------------------------------
# THROTTLE (how many jobs run at once)
# ---------------------------------------------------------------------------

$ThrottleLimit = 4   # bump to 6 if your laptop/network can handle it

# ---------------------------------------------------------------------------
# RESOLVE NPX PATH ONCE (jobs use full path)
# ---------------------------------------------------------------------------

try {
  $NpxPath = (Get-Command npx -ErrorAction Stop).Source
} catch {
  throw "npx not found on PATH. Node/npm install required."
}

# ---------------------------------------------------------------------------
# SITE LIST (requested sites + epgshare01.online scoped files)
# tag: used in output filenames
# site: repo site key
# channels_file: optional scoped list
# maxConnections: passed to grabber
# ---------------------------------------------------------------------------

$Sites = @(
  # Requested direct sites
  @{ tag="au_abc"            ; site="abc.net.au"                   ; channels_file=$null ; maxConnections=4 },
  @{ tag="us_directv"        ; site="directv.com"                  ; channels_file=$null ; maxConnections=4 },
  @{ tag="uk_virgin_go"      ; site="virgintvgo.virginmedia.com"   ; channels_file=$null ; maxConnections=4 },
  @{ tag="us_tvinsider"      ; site="tvinsider.com"                ; channels_file=$null ; maxConnections=4 },
  @{ tag="us_tvguide"        ; site="tvguide.com"                  ; channels_file=$null ; maxConnections=4 },
  @{ tag="uk_tv24"           ; site="tv24.co.uk"                   ; channels_file=$null ; maxConnections=4 },
  @{ tag="na_streaming_guides"; site="streamingtvguides.com"       ; channels_file=$null ; maxConnections=4 },
  @{ tag="us_pluto"          ; site="pluto.tv"                     ; channels_file=$null ; maxConnections=4 },
  @{ tag="na_plex"           ; site="plex.tv"                      ; channels_file=$null ; maxConnections=4 },
  @{ tag="uk_ee_player"      ; site="player.ee.co.uk"              ; channels_file=$null ; maxConnections=4 },

  # epgshare01.online (ONLY the scoped channel XMLs you listed)
  @{ tag="au_epgshare_au1"      ; site="epgshare01.online" ; channels_file="sites/epgshare01.online/epgshare01.online_AU1.channels.xml"        ; maxConnections=4 },
  @{ tag="ca_epgshare_ca1"      ; site="epgshare01.online" ; channels_file="sites/epgshare01.online/epgshare01.online_CA1.channels.xml"        ; maxConnections=4 },
  @{ tag="us_epgshare_us1"      ; site="epgshare01.online" ; channels_file="sites/epgshare01.online/epgshare01.online_US1.channels.xml"        ; maxConnections=4 },
  @{ tag="us_epgshare_locals2"  ; site="epgshare01.online" ; channels_file="sites/epgshare01.online/epgshare01.online_US_LOCALS2.channels.xml" ; maxConnections=4 },
  @{ tag="uk_epgshare_uk1"      ; site="epgshare01.online" ; channels_file="sites/epgshare01.online/epgshare01.online_UK1.channels.xml"        ; maxConnections=4 }
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

function Write-Log {
  param([string]$Message)
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "[$ts] $Message" | Tee-Object -FilePath $MainLog -Append
}

# Ensure folders exist
foreach ($f in @($CustomPath,$OutputPath,$LogPath,$DataPath,$CachePath)) {
  if (!(Test-Path $f)) { New-Item -ItemType Directory -Force -Path $f | Out-Null }
}

Write-Log "Starting $ScriptName"
Write-Log "BasePath=$BasePath"
Write-Log "OutputPath=$OutputPath"
Write-Log "LogPath=$LogPath"
Write-Log "ThrottleLimit=$ThrottleLimit"
Write-Log "SitesCount=$($Sites.Count)"
Write-Log "NpxPath=$NpxPath"

# ---------------------------------------------------------------------------
# JOB SCRIPTBLOCK: discovery grab (channels only) + inventory CSV
# ---------------------------------------------------------------------------

$JobBlock = {
  param($BasePath,$OutputPath,$LogPath,$SiteKey,$Tag,$MaxConnections,$ChannelsFile,$NpxPath)

  $discoverFile = Join-Path $OutputPath ("discover_{0}.xml" -f $Tag)
  $grabLog      = Join-Path $LogPath ("grab_{0}.log" -f $Tag)
  $invCsv       = Join-Path $OutputPath ("channels_{0}_inventory.csv" -f $Tag)

  Push-Location $BasePath
  try {
    # Build command args (CHANNELS ONLY via --days=0)
    $cmd = @(
      "tsx","scripts/commands/epg/grab.ts",
      "--site=$SiteKey",
      "--output=$discoverFile",
      "--maxConnections=$MaxConnections",
      "--days=0"
    )
    if ($ChannelsFile) { $cmd += "--channels=$ChannelsFile" }

    # Run grabber, tee to per-site log
    & $NpxPath @cmd 2>&1 | Tee-Object -FilePath $grabLog

    # Inventory extract if discover file created
    if (Test-Path $discoverFile) {
      [xml]$xml = Get-Content $discoverFile

      $rows = foreach ($c in $xml.tv.channel) {
        [pscustomobject]@{
          tag  = $Tag
          id   = $c.id
          name = ($c.'display-name' | Select-Object -First 1).'#text'
        }
      }

      $rows | Sort-Object id -Unique |
        Export-Csv $invCsv -NoTypeInformation -Encoding UTF8
    }
  }
  finally {
    Pop-Location
  }

  # Return a small job summary object
  [pscustomobject]@{
    tag = $Tag
    site = $SiteKey
    discoverFile = $discoverFile
    inventoryFile = $invCsv
    discoverExists = (Test-Path $discoverFile)
    inventoryExists = (Test-Path $invCsv)
  }
}

# ---------------------------------------------------------------------------
# START JOBS WITH THROTTLING
# ---------------------------------------------------------------------------

$jobs = @()

foreach ($s in $Sites) {

  # Throttle: wait until running jobs < limit
  while ( (@($jobs | Where-Object { $_.State -eq 'Running' })).Count -ge $ThrottleLimit ) {
    Start-Sleep -Seconds 2
  }

  Write-Log "Queueing job tag=$($s.tag) site=$($s.site) channels=$($s.channels_file)"

  $jobs += Start-Job -ScriptBlock $JobBlock -ArgumentList `
    $BasePath,$OutputPath,$LogPath, `
    $s.site,$s.tag,$s.maxConnections,$s.channels_file, `
    $NpxPath
}

Write-Log "All jobs queued. Waiting for completion..."

# ---------------------------------------------------------------------------
# WAIT FOR ALL JOBS, COLLECT SUMMARIES
# ---------------------------------------------------------------------------

$results = Receive-Job -Job (Wait-Job -Job $jobs) -Keep
$null = $jobs | Remove-Job

foreach ($r in $results) {
  Write-Log ("JobDone tag={0} site={1} discoverExists={2} inventoryExists={3}" -f `
    $r.tag,$r.site,$r.discoverExists,$r.inventoryExists)
}

# ---------------------------------------------------------------------------
# CONSOLIDATE INVENTORIES (only after all jobs are done)
# ---------------------------------------------------------------------------

Write-Log "Starting consolidation → consolidated_inventory.csv"

$InventoryFiles = Get-ChildItem $OutputPath -Filter "channels_*_inventory.csv" |
                  Where-Object { $_.Length -gt 0 }

$consolidatedCsv = Join-Path $OutputPath "consolidated_inventory.csv"
$resultRows = @()

foreach ($file in $InventoryFiles) {
  $csv = Import-Csv $file.FullName
  foreach ($row in $csv) {
    $siteKey = ($Sites | Where-Object { $_.tag -eq $row.tag }).site
    $resultRows += [pscustomobject]@{
      tag     = $row.tag
      site    = $siteKey
      id      = $row.id
      name    = $row.name
      source  = $file.Name
    }
  }
}

$resultRows | Sort-Object tag,name |
  Export-Csv $consolidatedCsv -NoTypeInformation -Encoding UTF8

Write-Log "Consolidated CSV created: $consolidatedCsv"

# ---------------------------------------------------------------------------
# CONSOLIDATE DISCOVER XML FILES (CHANNELS ONLY)
# ---------------------------------------------------------------------------

Write-Log "Starting consolidation → consolidated_discover_channels_only.xml"

$discoverFiles = Get-ChildItem $OutputPath -Filter "discover_*.xml" |
                 Where-Object { $_.Length -gt 0 }

$consolidatedXml = Join-Path $OutputPath "consolidated_discover_channels_only.xml"

$xmlMaster = New-Object System.Xml.XmlDocument
$root = $xmlMaster.CreateElement("tv")
$xmlMaster.AppendChild($root) | Out-Null

foreach ($file in $discoverFiles) {
  try {
    [xml]$x = Get-Content $file.FullName

    # Channels only (no programmes merged)
    foreach($channel in $x.tv.channel){
      $root.AppendChild($xmlMaster.ImportNode($channel,$true)) | Out-Null
    }
  } catch {
    Write-Log "WARN: could not merge $($file.Name): $($_.Exception.Message)"
  }
}

$xmlMaster.Save($consolidatedXml)
Write-Log "Consolidated XML created: $consolidatedXml"

Write-Log "Completed $ScriptName"
Write-Host "`nDONE! Parallel channel discovery + inventories + consolidated outputs created:`n$consolidatedCsv`n$consolidatedXml`n"
