#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Applies advanced audit policy subcategories.
    Fixes V-278942 to V-278947:
      File System (Success + Failure)
      Handle Manipulation (Success + Failure)
      Registry (Success + Failure)
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

function Write-OK   ($m) { Write-Host "  [OK]   $m" -ForegroundColor Green  }
function Write-Fail ($m) { Write-Host "  [FAIL] $m" -ForegroundColor Red    }
function Write-Warn ($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }

Write-Host "`n=== Applying Advanced Audit Policy (V-278942 to V-278947) ===" -ForegroundColor Cyan

$ErrorCount = 0

# -----------------------------------------------------------------------
# Ensure advanced audit policy is NOT overridden by basic audit policy
# This registry key must be 1 or advanced auditpol settings are ignored
# -----------------------------------------------------------------------
$regPath = 'HKLM:\SYSTEM\CurrentControlSet\Control\Lsa'
$current = (Get-ItemProperty -Path $regPath -Name 'SCENoApplyLegacyAuditPolicy' -ErrorAction SilentlyContinue).SCENoApplyLegacyAuditPolicy
if ($current -ne 1) {
    Set-ItemProperty -Path $regPath -Name 'SCENoApplyLegacyAuditPolicy' -Value 1 -Type DWord
    Write-OK "Set SCENoApplyLegacyAuditPolicy = 1 (advanced audit takes precedence)"
} else {
    Write-OK "SCENoApplyLegacyAuditPolicy already set correctly"
}

# -----------------------------------------------------------------------
# Apply subcategory settings via auditpol
# -----------------------------------------------------------------------
$auditSettings = @(
    @{ Subcategory = "File System";          STIG = "V-278942/V-278943"; Success = "enable"; Failure = "enable" },
    @{ Subcategory = "Handle Manipulation";  STIG = "V-278944/V-278945"; Success = "enable"; Failure = "enable" },
    @{ Subcategory = "Registry";             STIG = "V-278946/V-278947"; Success = "enable"; Failure = "enable" }
)

foreach ($s in $auditSettings) {
    Write-Host "  Setting [$($s.STIG)]: '$($s.Subcategory)' Success=$($s.Success) Failure=$($s.Failure)"

    $out = & auditpol /set /subcategory:"$($s.Subcategory)" /success:$($s.Success) /failure:$($s.Failure) 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-OK "$($s.Subcategory) set successfully"
    } else {
        Write-Fail "$($s.Subcategory) failed (exit $LASTEXITCODE): $out"
        $ErrorCount++
    }
}

# -----------------------------------------------------------------------
# Verification
# -----------------------------------------------------------------------
Write-Host "`n--- Verification ---" -ForegroundColor Cyan

$subcategories = @("File System", "Handle Manipulation", "Registry")
foreach ($sub in $subcategories) {
    $result = & auditpol /get /subcategory:"$sub" 2>&1
    $line   = $result | Select-String $sub
    if ($line) {
        if ($line -match 'Success and Failure') {
            Write-OK "$sub : Success and Failure [COMPLIANT]"
        } elseif ($line -match 'Success') {
            Write-Warn "$sub : Success only [PARTIAL - Failure missing]"
            $ErrorCount++
        } elseif ($line -match 'Failure') {
            Write-Warn "$sub : Failure only [PARTIAL - Success missing]"
            $ErrorCount++
        } else {
            Write-Fail "$sub : No Auditing [NOT COMPLIANT]"
            $ErrorCount++
        }
    } else {
        Write-Warn "$sub : Could not parse auditpol output"
    }
}

Write-Host "`n=== Audit Policy Complete (errors: $ErrorCount) ===" -ForegroundColor Cyan
exit $ErrorCount
