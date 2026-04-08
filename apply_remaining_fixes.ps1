#Requires -RunAsAdministrator
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

function Write-OK   ($m) { Write-Host "  [OK]   $m" -ForegroundColor Green  }
function Write-Fail ($m) { Write-Host "  [FAIL] $m" -ForegroundColor Red    }
function Write-Warn ($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Write-Info ($m) { Write-Host "  [INFO] $m" -ForegroundColor Cyan   }
function Write-Section ($m) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host " $m" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

$ErrorCount = 0

# -----------------------------------------------------------------------
# V-254258: Passwords must expire
# -----------------------------------------------------------------------
Write-Section "V-254258: Configure password expiry"

Get-LocalUser | Where-Object { $_.Enabled } | ForEach-Object {
    if ($_.PasswordNeverExpires) {
        try {
            Set-LocalUser -Name $_.Name -PasswordNeverExpires $false
            Write-OK "Password expiry enabled: $($_.Name)"
        } catch {
            Write-Fail "Failed: $($_.Name)"
            $ErrorCount++
        }
    }
}

try {
    net accounts /maxpwage:60 | Out-Null
    Write-OK "Max password age set to 60 days"
} catch {
    Write-Fail "Failed to set password policy"
    $ErrorCount++
}

# -----------------------------------------------------------------------
# V-254251: FIX C:\ ROOT ACL
# -----------------------------------------------------------------------
Write-Section "V-254251: Fix C:\ permissions"

try {
    icacls "C:\" /inheritance:r | Out-Null
    icacls "C:\" /remove "Everyone" 2>$null
    icacls "C:\" /remove "Authenticated Users" 2>$null

    icacls "C:\" /grant:r "SYSTEM:(OI)(CI)(F)" | Out-Null
    icacls "C:\" /grant:r "Administrators:(OI)(CI)(F)" | Out-Null
    icacls "C:\" /grant:r "Users:(OI)(CI)(RX)" | Out-Null
    icacls "C:\" /grant:r "CREATOR OWNER:(OI)(CI)(IO)(F)" | Out-Null

    Write-OK "C:\ ACL set correctly"
} catch {
    Write-Fail "ACL fix failed"
    $ErrorCount++
}

# -----------------------------------------------------------------------
# V-254261: Remove cert/install artifacts
# -----------------------------------------------------------------------
Write-Section "V-254261: Cleanup artifacts"

Get-Process msiexec -ErrorAction SilentlyContinue | Stop-Process -Force

$searchPaths = @(
    "C:\Users\packer_user\hardening",
    "C:\Users\packer_user\Desktop",
    "C:\DoD_Certs",
    "C:\Windows\Temp",
    "$env:TEMP"
)

# Remove .p7b (priority)
foreach ($path in $searchPaths) {
    if (!(Test-Path $path)) { continue }

    Get-ChildItem $path -Recurse -Include *.p7b -Force -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            takeown /F $_.FullName /A /R /D Y | Out-Null
            icacls $_.FullName /grant Administrators:F /T /Q | Out-Null
            Remove-Item $_.FullName -Force
            Write-OK "Removed P7B: $($_.FullName)"
        } catch {
            Write-Warn "Failed: $($_.FullName)"
        }
    }
}

# Remove DoD folder
if (Test-Path "C:\DoD_Certs") {
    Remove-Item "C:\DoD_Certs" -Recurse -Force -ErrorAction SilentlyContinue
    Write-OK "Removed DoD_Certs"
}

# -----------------------------------------------------------------------
# V-254447 & V-254448: Rename accounts
# -----------------------------------------------------------------------
Write-Section "Rename built-in accounts"

try {
    $admin = Get-LocalUser | Where-Object SID -like "*-500"
    if ($admin.Name -ne "AdminRenamed") {
        Rename-LocalUser -Name $admin.Name -NewName "AdminRenamed"
    }
    Write-OK "Admin OK"
} catch {
    Write-Fail "Admin rename failed"
    $ErrorCount++
}

try {
    $guest = Get-LocalUser | Where-Object SID -like "*-501"
    if ($guest.Name -ne "GuestRenamed") {
        Rename-LocalUser -Name $guest.Name -NewName "GuestRenamed"
    }
    Write-OK "Guest OK"
} catch {
    Write-Fail "Guest rename failed"
    $ErrorCount++
}

# -----------------------------------------------------------------------
# VERIFY
# -----------------------------------------------------------------------
Write-Section "Verification"

$adminCheck = Get-LocalUser | Where-Object SID -like "*-500"
$guestCheck = Get-LocalUser | Where-Object SID -like "*-501"

if ($adminCheck.Name -eq "AdminRenamed") {
    Write-OK "Admin verified"
} else {
    Write-Fail "Admin not renamed"
    $ErrorCount++
}

if ($guestCheck.Name -eq "GuestRenamed") {
    Write-OK "Guest verified"
} else {
    Write-Fail "Guest not renamed"
    $ErrorCount++
}

# -----------------------------------------------------------------------
# FINAL VALIDATION
# -----------------------------------------------------------------------
Write-Section "Final Validation"

net accounts | Select-String "Maximum password age"

$left = Get-ChildItem C:\Users,C:\Windows\Temp,C:\DoD_Certs -Recurse `
    -Include *.p7b,*.cer,*.crt,*.der,*.msi `
    -ErrorAction SilentlyContinue

if ($left.Count -eq 0) {
    Write-OK "No leftover files"
} else {
    Write-Fail "Files still exist"
    $ErrorCount++
}

# -----------------------------------------------------------------------
# SUMMARY
# -----------------------------------------------------------------------
Write-Section "SUMMARY"

if ($ErrorCount -eq 0) {
    Write-Host "ALL STIG FIXES APPLIED " -ForegroundColor Green
} else {
    Write-Host "$ErrorCount issues remain " -ForegroundColor Yellow
}

exit $ErrorCount
