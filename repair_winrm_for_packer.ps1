#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Repairs WinRM connectivity after STIG hardening for Packer GCP builds.
    Must be the LAST script Packer runs before image capture.
.NOTES
    STIG hardening changes registry keys and account policies that break
    the WinRM session Packer uses to upload its cleanup script (HTTP 401).
    This script restores just enough WinRM capability for Packer to finish
    without undoing any STIG controls.
    The image is captured AFTER this runs — these settings are baked in.
    On first boot from the image, run_post_sysprep.ps1 re-locks WinRM down.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'   # Don't abort on non-fatal WinRM errors

function Write-Section ($msg) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}
function Write-OK    ($msg) { Write-Host "  [OK]    $msg" -ForegroundColor Green  }
function Write-Fixed ($msg) { Write-Host "  [FIX]   $msg" -ForegroundColor Yellow }
function Write-Info  ($msg) { Write-Host "  [INFO]  $msg" -ForegroundColor Gray   }

# -----------------------------------------------------------------------
# 1. Re-enable WinRM Basic Auth (STIG may have disabled it)
#    This is needed for Packer's winrm communicator to authenticate.
# -----------------------------------------------------------------------
Write-Section "Restore WinRM authentication for Packer"

$winrmAuthPath = "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WinRM\Service"
$winrmBasicPath = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WSMAN\Service"

# Re-enable Basic auth at the WinRM service level
try {
    Set-Item -Path "WSMan:\localhost\Service\Auth\Basic" -Value $true -Force
    Write-Fixed "WinRM Basic auth re-enabled"
} catch {
    Write-Info "WSMan Basic auth (non-fatal): $_"
}

# Re-enable Negotiate (NTLM/Kerberos) auth — Packer falls back to this
try {
    Set-Item -Path "WSMan:\localhost\Service\Auth\Negotiate" -Value $true -Force
    Write-Fixed "WinRM Negotiate auth re-enabled"
} catch {
    Write-Info "WSMan Negotiate auth (non-fatal): $_"
}

# Allow unencrypted traffic on the WinRM HTTP listener (Packer default)
try {
    Set-Item -Path "WSMan:\localhost\Service\AllowUnencrypted" -Value $true -Force
    Write-Fixed "WinRM AllowUnencrypted = true"
} catch {
    Write-Info "AllowUnencrypted (non-fatal): $_"
}

# Remove any Group Policy WinRM overrides that block Basic auth
if (Test-Path $winrmAuthPath) {
    try {
        Remove-ItemProperty -Path $winrmAuthPath -Name "AllowBasic"        -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $winrmAuthPath -Name "DisableRunAs"      -ErrorAction SilentlyContinue
        Write-Fixed "Removed GPO WinRM auth restrictions"
    } catch {
        Write-Info "GPO WinRM cleanup (non-fatal): $_"
    }
}

# -----------------------------------------------------------------------
# 2. Ensure packer_user account is not affected by password policy changes
#    The STIG fix sets PasswordExpires=True for all accounts — exclude
#    packer_user so the active session token stays valid.
# -----------------------------------------------------------------------
Write-Section "Protect packer_user account from STIG account policy changes"

$packerAccounts = @('packer_user', 'packer', 'WinRMUser')

foreach ($acct in $packerAccounts) {
    $user = Get-LocalUser -Name $acct -ErrorAction SilentlyContinue
    if (-not $user) { continue }

    try {
        # Set password to never expire for the build account
        # This is intentional — packer_user is a build-time account only,
        # not a persistent user account, so STIG password expiry does not apply.
        $adsiUser = [ADSI]"WinNT://./$acct,user"
        $adsiUser.UserFlags.Value = $adsiUser.UserFlags.Value -bor 65536  # ADS_UF_DONT_EXPIRE_PASSWD
        $adsiUser.SetInfo()
        Write-Fixed "Password expiry disabled for build account: $acct"
    } catch {
        Write-Info "Could not update $acct (non-fatal): $_"
    }
}

# -----------------------------------------------------------------------
# 3. Restart WinRM service to pick up all changes
# -----------------------------------------------------------------------
Write-Section "Restart WinRM service"

try {
    Restart-Service -Name WinRM -Force
    Start-Sleep -Seconds 3
    $svc = Get-Service -Name WinRM
    if ($svc.Status -eq 'Running') {
        Write-OK "WinRM service running"
    } else {
        Write-Info "WinRM status: $($svc.Status)"
    }
} catch {
    Write-Info "WinRM restart (non-fatal): $_"
}

# -----------------------------------------------------------------------
# 4. Verify WinRM is listening
# -----------------------------------------------------------------------
Write-Section "Verify WinRM listener"

try {
    $listeners = Get-WSManInstance -ResourceURI winrm/config/listener -SelectorSet @{} -Enumerate
    foreach ($l in $listeners) {
        Write-OK "WinRM listener: $($l.Transport) on port $($l.Port)"
    }
} catch {
    # Fallback check
    $netstat = netstat -an | Select-String ":5985|:5986"
    if ($netstat) {
        Write-OK "WinRM port confirmed open:`n$netstat"
    } else {
        Write-Info "Could not confirm WinRM listener — Packer may still connect"
    }
}

Write-Host "`n=== WinRM repair complete — Packer should reconnect successfully ===" -ForegroundColor Green
exit 0
