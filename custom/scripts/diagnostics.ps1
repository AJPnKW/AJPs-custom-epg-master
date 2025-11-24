<#
diagnostics.ps1
Purpose: Detect PowerShell profile overrides/aliases that break scripts (esp Write-Host),
         confirm PS version, and print quick environment checks.
Version: 1.0
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

function HR($t){ Microsoft.PowerShell.Utility\Write-Host "`n==== $t ====" }

HR "PowerShell Version"
$PSVersionTable | Format-List | Out-String | Microsoft.PowerShell.Utility\Write-Host

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Microsoft.PowerShell.Utility\Write-Host "[WARN] You are NOT on PowerShell 7+. ForEach-Object -Parallel will fail."
} else {
    Microsoft.PowerShell.Utility\Write-Host "[OK] PowerShell 7+ detected."
}

HR "Write-Host command chain"
Get-Command Write-Host -All | Select-Object Name,CommandType,Source,Definition |
    Format-Table -AutoSize | Out-String | Microsoft.PowerShell.Utility\Write-Host

$wh = Get-Command Write-Host -All | Select-Object -First 1
if ($wh.CommandType -ne "Cmdlet") {
    Microsoft.PowerShell.Utility\Write-Host "[WARN] Write-Host is overridden by a $($wh.CommandType). This can break scripts."
    Microsoft.PowerShell.Utility\Write-Host "       Fix by removing/reverting in your profile, or scripts must use Microsoft.PowerShell.Utility\Write-Host."
} else {
    Microsoft.PowerShell.Utility\Write-Host "[OK] Write-Host is the built-in cmdlet."
}

HR "Aliases that commonly cause trouble"
"Write-Host","Write-Output","Out-Host","ForEach-Object","Format-Table" | ForEach-Object {
    $cmd = Get-Command $_ -All | Select-Object -First 1
    Microsoft.PowerShell.Utility\Write-Host ("{0,-15} => {1}" -f $_, $cmd.CommandType)
}

HR "Profile locations"
$profiles = @(
    $PROFILE.CurrentUserAllHosts,
    $PROFILE.CurrentUserCurrentHost,
    $PROFILE.AllUsersAllHosts,
    $PROFILE.AllUsersCurrentHost
)
foreach ($p in $profiles) {
    if ($p) {
        $exists = Test-Path $p
        Microsoft.PowerShell.Utility\Write-Host ("{0}  (exists={1})" -f $p, $exists)
    }
}

HR "If Write-Host is overridden, it is usually in one of these profiles"
Microsoft.PowerShell.Utility\Write-Host "Open the profile file and search for: function Write-Host or Set-Alias Write-Host"

HR "Done."
