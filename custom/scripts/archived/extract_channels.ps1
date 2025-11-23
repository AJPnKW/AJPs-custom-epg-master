<#
Script Name: extract_channels.ps1
Purpose: Extract channel inventories from discover_*.xml files
Author: Andrew J. Pearen (personal EPG project)
Location: custom/scripts/
Log: custom/logs/extract_channels.log
Output: custom/output/
#>

# --- CONFIG PATHS -------------------------------------------------------------

$BasePath   = "C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
$CustomPath = "$BasePath\custom"
$OutputPath = "$CustomPath\output"
$LogPath    = "$CustomPath\logs"
$DataPath   = "$CustomPath\data"
$CachePath  = "$CustomPath\cache"
$ScriptName = "extract_channels.ps1"
$LogFile    = "$LogPath\extract_channels.log"

# --- START LOGGING ------------------------------------------------------------

$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$Timestamp] Starting $ScriptName" | Tee-Object -FilePath $LogFile -Append
"BasePath: $BasePath" | Tee-Object -FilePath $LogFile -Append
"OutputPath: $OutputPath" | Tee-Object -FilePath $LogFile -Append

# --- ENSURE FOLDERS EXIST -----------------------------------------------------

$folders = @($OutputPath, $LogPath, $DataPath, $CachePath)

foreach ($f in $folders) {
    if (!(Test-Path $f)) {
        New-Item -ItemType Directory -Path $f | Out-Null
        "Created folder: $f" | Tee-Object -FilePath $LogFile -Append
    }
}

# --- FUNCTION: Extract Channels ------------------------------------------------

function Export-Channels {
    param(
        [string]$XmlPath,
        [string]$CsvOut
    )

    try {
        [xml]$xml = Get-Content $XmlPath

        $rows = foreach ($c in $xml.tv.channel) {
            [pscustomobject]@{
                id   = $c.id
                name = ($c.'display-name' | Select-Object -First 1).'#text'
            }
        }

        $rows | Sort-Object id -Unique | Export-Csv $CsvOut -NoTypeInformation -Encoding UTF8

        $msg = "Extracted channels from $XmlPath → $CsvOut (Count: $($rows.Count))"
        Write-Host $msg
        $msg | Tee-Object -FilePath $LogFile -Append
    }
    catch {
        $err = "ERROR processing $XmlPath: $_"
        Write-Host $err
        $err | Tee-Object -FilePath $LogFile -Append
    }
}

# --- DISCOVERY FILE SCAN -------------------------------------------------------

"Scanning for discover_*.xml files..." | Tee-Object -FilePath $LogFile -Append

$discoverFiles = Get-ChildItem -Path $OutputPath -Filter "discover_*.xml"

if ($discoverFiles.Count -eq 0) {
    "No discovery XML files found in $OutputPath" | Tee-Object -FilePath $LogFile -Append
    exit
}

"Found $($discoverFiles.Count) discovery XML file(s)." | Tee-Object -FilePath $LogFile -Append

# --- PROCESS EACH DISCOVERY FILE ----------------------------------------------

foreach ($file in $discoverFiles) {
    $inputFile  = $file.FullName
    $countryTag = ($file.BaseName -replace "discover_", "")
    $csvOut     = "$OutputPath\channels_${countryTag}_inventory.csv"

    "Processing $inputFile..." | Tee-Object -FilePath $LogFile -Append
    Export-Channels -XmlPath $inputFile -CsvOut $csvOut
}

# --- END -----------------------------------------------------------------------

$EndTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$EndTime] Completed $ScriptName" | Tee-Object -FilePath $LogFile -Append

Write-Host "`nDone! Channel inventory extraction complete."
