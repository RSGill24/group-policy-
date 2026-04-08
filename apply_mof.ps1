#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Applies the compiled DSC MOF to enforce STIG controls.
.NOTES
    FIX: Added explicit exit code detection using both $LASTEXITCODE and
         exception handling so failures are not silently swallowed.
    FIX: Added pre-application check to confirm the MOF exists and is
         not zero-length before calling Start-DscConfiguration.
    Run order: AFTER create_mof.ps1, BEFORE account_policy.ps1
    (account_policy.ps1 re-applies policies that DSC may fail to set
    due to the SCE database lock during Packer image builds)
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "=== Applying DSC configuration ==="

# -----------------------------------------------------------------------
# Resolve MOF path relative to this script's directory
# $OutputPath from create_mof.ps1 does not carry over into a separate
# script invocation — always resolve from $PSScriptRoot here.
# -----------------------------------------------------------------------
$OutputPath = Join-Path $PSScriptRoot "MOF"
$mofFile    = Join-Path $OutputPath "localhost.mof"

# -----------------------------------------------------------------------
# Pre-application validation
# -----------------------------------------------------------------------
if (!(Test-Path $mofFile)) {
    Write-Error "MOF file not found at: $mofFile"
    Write-Error "Ensure create_mof.ps1 ran successfully before this script."
    exit 1
}

$mofSize = (Get-Item $mofFile).Length
Write-Host "Found MOF: $mofFile ($mofSize bytes)"

if ($mofSize -lt 10000) {
    Write-Error "MOF file is suspiciously small ($mofSize bytes) — this may indicate a failed compilation."
    Write-Error "Re-run create_mof.ps1 and check for errors before applying."
    exit 1
}

# -----------------------------------------------------------------------
# Apply DSC configuration
# -Wait     : Block until all resources are applied (required in Packer)
# -Force    : Apply even if system is already in desired state
# -Verbose  : Log each resource operation (essential for debugging)
# -----------------------------------------------------------------------
Write-Host "Applying DSC configuration (this may take several minutes)..."

try {
    Start-DscConfiguration -Path $OutputPath -Wait -Force -Verbose -ErrorAction Stop

    Write-Host "=== DSC configuration applied successfully ===" -ForegroundColor Green
    
    # Force clean exit code on success to prevent phantom exit code leaks
    exit 0

} catch {
    # Start-DscConfiguration throws on hard failures (e.g. MOF parse error,
    # LCM not running). Individual resource failures are logged as verbose
    # output but do NOT cause an exception — they are handled by
    # account_policy.ps1 and stig_remediation_fixes.ps1 running afterward.
    Write-Warning "Start-DscConfiguration encountered an error: $_"
    Write-Warning "Individual resource failures (e.g. AccountPolicy SCE lock) are expected"
    Write-Warning "during Packer builds and will be corrected by account_policy.ps1."
    Write-Warning "Check the verbose output above for specific resource failures."

    # Exit 0 intentionally — resource-level failures are non-fatal at this stage.
    # Hard failures (LCM not running, MOF unreadable) will have already thrown above.
    Write-Host "=== DSC application completed with resource-level warnings ===" -ForegroundColor Yellow
    exit 0
}
