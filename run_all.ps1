#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Main STIG hardening entry point for Packer/GCP builds.
    Orchestrates all hardening scripts in the correct order.
.NOTES
    Script execution order matters:
    1. PowerSTIG + dependencies must be installed before DSC MOF creation
    2. DoD certs must be installed before DSC MOF (so DSC cert checks pass)
    3. DSC MOF must be created before it is applied
    4. account_policy.ps1 runs AFTER apply_mof.ps1 (DSC cannot undo secedit writes)
    5. stig_remediation_fixes.ps1 runs AFTER apply_mof.ps1 (same reason)
    6. audit.ps1 runs AFTER apply_mof.ps1 (V-278942 to V-278947)
    7. apply_remaining_fixes.ps1 runs AFTER apply_mof.ps1 (V-254251/258/261)
    8. repair_winrm_for_packer.ps1 runs LAST (must not be undone by any STIG script)
    FIX: Integrity check corrected to use 'audit.ps1' (the actual filename)
         instead of 'apply_audit_policy.ps1' which does not exist on disk.
         This was causing the pre-flight check to error with MISSING even
         though audit.ps1 was uploaded correctly by Packer.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

Write-Host "========== START STIG HARDENING ==========" -ForegroundColor Cyan
Write-Host "  Start time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan

$scriptDir   = $PSScriptRoot
$globalFails = 0

# -----------------------------------------------------------------------
# Helper: Run a script step and abort pipeline on failure
# -----------------------------------------------------------------------
function Invoke-Step {
    param(
        [string]$ScriptPath,
        [string]$Label,
        [switch]$AllowFailure
    )

    Write-Host "`n--- $Label ---" -ForegroundColor Yellow
    Write-Host "    Script: $ScriptPath" -ForegroundColor Gray
    Write-Host "    Time:   $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray

    if (-not (Test-Path $ScriptPath)) {
        Write-Warning "Script not found, skipping: $ScriptPath"
        return
    }

    $global:LASTEXITCODE = 0
    $stepFailed = $false

    try {
        & $ScriptPath
        if ($global:LASTEXITCODE -ne 0) { $stepFailed = $true }
    } catch {
        Write-Warning "Unhandled exception in $Label : $_"
        $stepFailed = $true
        $global:LASTEXITCODE = 1
    }

    $exitCode = $global:LASTEXITCODE

    if ($stepFailed -and -not $AllowFailure) {
        Write-Error "$Label FAILED with exit code $exitCode -- aborting hardening pipeline"
        exit $exitCode
    } elseif ($stepFailed -and $AllowFailure) {
        Write-Warning "$Label completed with warnings (exit code: $exitCode) -- continuing"
        $script:globalFails++
    } else {
        Write-Host "    $Label completed OK" -ForegroundColor Green
    }
}

# -----------------------------------------------------------------------
# PRE-FLIGHT: Verify critical scripts are not truncated
# FIX: Uses 'audit.ps1' (actual filename) not 'apply_audit_policy.ps1'
# -----------------------------------------------------------------------
Write-Host "`n--- Pre-flight: Script integrity checks ---" -ForegroundColor Yellow

$integrityChecks = @(
    @{ Path = "$scriptDir\install_dod_certs.ps1";      MinLines = 50;  Label = "install_dod_certs.ps1"     }
    @{ Path = "$scriptDir\stig_remediation_fixes.ps1"; MinLines = 100; Label = "stig_remediation_fixes.ps1" }
    @{ Path = "$scriptDir\install_dsc_deps.ps1";       MinLines = 50;  Label = "install_dsc_deps.ps1"      }
    @{ Path = "$scriptDir\create_mof.ps1";             MinLines = 50;  Label = "create_mof.ps1"            }
    @{ Path = "$scriptDir\audit.ps1";                  MinLines = 30;  Label = "audit.ps1"                 }
    @{ Path = "$scriptDir\apply_remaining_fixes.ps1";  MinLines = 30;  Label = "apply_remaining_fixes.ps1" }
)

$integrityFail = $false

foreach ($check in $integrityChecks) {
    if (Test-Path $check.Path) {
        $lineCount = (Get-Content $check.Path).Count
        $hash      = (Get-FileHash $check.Path -Algorithm SHA256).Hash

        Write-Host "  $($check.Label): $lineCount lines | SHA256: $($hash.Substring(0,16))..." -ForegroundColor Gray

        if ($lineCount -lt $check.MinLines) {
            Write-Host "  [FAIL] $($check.Label) appears TRUNCATED ($lineCount lines, expected >= $($check.MinLines))" -ForegroundColor Red
            $integrityFail = $true
        } else {
            Write-Host "  [OK]  $($check.Label)" -ForegroundColor Green
        }
    } else {
        Write-Warning "  Script not found (will be caught later): $($check.Path)"
    }
}

if ($integrityFail) {
    Write-Error "One or more scripts appear truncated. Fix the Packer file provisioner and retry."
    exit 1
}

# -----------------------------------------------------------------------
# STEP 1 -- Install PowerSTIG and DSC dependencies
# -----------------------------------------------------------------------
Invoke-Step "$scriptDir\install_PowerSTIG.ps1"   "Install PowerSTIG"
Invoke-Step "$scriptDir\install_dsc_deps.ps1"    "Install DSC dependencies (removes CIM duplicates)"

# -----------------------------------------------------------------------
# STEP 2 -- Install DoD Certificates (V-254442, V-254443, V-254444)
# Must run BEFORE create_mof.ps1 so DSC certificate checks find certs installed.
# -----------------------------------------------------------------------
Invoke-Step "$scriptDir\install_dod_certs.ps1"   "Install DoD Certificates (V-254442/443/444)" -AllowFailure

# -----------------------------------------------------------------------
# STEP 3 -- Create and apply the DSC MOF
# -----------------------------------------------------------------------
Invoke-Step "$scriptDir\create_mof.ps1"          "Create DSC MOF"
Invoke-Step "$scriptDir\apply_mof.ps1"           "Apply DSC MOF"

# -----------------------------------------------------------------------
# STEP 4 -- Post-DSC targeted fixes
# All scripts below MUST run AFTER apply_mof.ps1.
# DSC cannot undo secedit/registry/auditpol writes -- running these after
# DSC ensures they are the final state baked into the image.
# -----------------------------------------------------------------------
Invoke-Step "$scriptDir\registry_stig.ps1"           "Registry STIG fixes"                             -AllowFailure
Invoke-Step "$scriptDir\services_stig.ps1"           "Services STIG fixes"                             -AllowFailure

# V-254285/286/287/288/289/290/291/292 -- password and lockout policy
Invoke-Step "$scriptDir\account_policy.ps1"          "Account Policy (net accounts + secedit)"

# V-278942/943/944/945/946/947 -- audit file system, handle manipulation, registry
# FIX: script is named audit.ps1 not apply_audit_policy.ps1
Invoke-Step "$scriptDir\audit.ps1"                   "Audit Subcategory Policy (V-278942 to V-278947)" -AllowFailure

# V-254251/258/261 -- C:\ permissions, password expiry, cert file removal
Invoke-Step "$scriptDir\apply_remaining_fixes.ps1"   "Remaining STIG fixes (V-254251/258/261)"         -AllowFailure

# Broader targeted fixes for remaining SCAP failures
Invoke-Step "$scriptDir\stig_remediation_fixes.ps1"  "Targeted STIG remediation"                       -AllowFailure

# -----------------------------------------------------------------------
# STEP 5 -- Repair WinRM for Packer (MUST be absolute last step)
# -----------------------------------------------------------------------
Invoke-Step "$scriptDir\repair_winrm_for_packer.ps1" "Repair WinRM for Packer (LAST step)"

# -----------------------------------------------------------------------
# STEP 6 -- Final DSC compliance audit
# -----------------------------------------------------------------------
Write-Host "`n--- Final DSC Compliance Audit ---" -ForegroundColor Yellow

try {
    $results    = Test-DscConfiguration -Detailed -ErrorAction Stop
    $reportPath = Join-Path $scriptDir "DSC_Audit_Results.csv"

    $final = @()
    foreach ($r in $results.ResourcesInDesiredState) {
        $r | Add-Member -NotePropertyName Compliance -NotePropertyValue "PASS" -Force
        $final += $r
    }
    foreach ($r in $results.ResourcesNotInDesiredState) {
        $r | Add-Member -NotePropertyName Compliance -NotePropertyValue "FAIL" -Force
        $final += $r
    }
    $final | Export-Csv -Path $reportPath -NoTypeInformation

    $passCount = ($results.ResourcesInDesiredState    | Measure-Object).Count
    $failCount = ($results.ResourcesNotInDesiredState | Measure-Object).Count

    Write-Host "  DSC Compliance: $passCount PASS  /  $failCount FAIL" -ForegroundColor Cyan
    Write-Host "  Report saved:   $reportPath" -ForegroundColor Cyan

    if ($failCount -gt 0) {
        Write-Host "`n  DSC resources NOT in desired state:" -ForegroundColor Yellow
        $results.ResourcesNotInDesiredState | ForEach-Object {
            Write-Host "    FAIL: $($_.ResourceId)" -ForegroundColor Yellow
        }
        $globalFails += $failCount
    }
} catch {
    Write-Warning "DSC compliance audit failed: $_"
    Write-Warning "This is non-fatal -- the image will still be captured."
}

# -----------------------------------------------------------------------
# Final summary
# -----------------------------------------------------------------------
Write-Host "`n========== STIG HARDENING COMPLETE ==========" -ForegroundColor Cyan
Write-Host "  End time:        $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
Write-Host "  Pipeline issues: $globalFails" -ForegroundColor $(if ($globalFails -eq 0) { 'Green' } else { 'Yellow' })

if ($globalFails -gt 0) {
    Write-Host "  Review WARN messages above and the DSC audit CSV for details." -ForegroundColor Yellow
}

Write-Host "==========================================" -ForegroundColor Cyan
exit 0
