# ==============================
# BigFix Agent Installation Script
# ==============================
# Same approach as Trellix — install unregistered, delete computer ID,
# stop service. Client activates on first boot with fresh computer ID.
# Each VM spawned from this image gets a unique BigFix Agent ID.
# ==============================

$bucketPath   = "gs://nmfs-trusted-images-packages/bigfix/BESClientsetup.exe"
$downloadPath = "C:\Windows\Temp\BESClientsetup.exe"
$logPath      = "C:\Windows\Temp\bigfix_install.log"
$installPath  = "C:\Program Files (x86)\BigFix Enterprise\BES Client"
$serviceNames = @("BESClient")

# ----------------------------
# 1. Uninstall existing
# ----------------------------
Write-Host "[1/6] Checking for existing BigFix Agent..." -ForegroundColor Yellow

$bigfixReg = $null
$regEntries  = @(Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*" `
    -ErrorAction SilentlyContinue)
$regEntries += @(Get-ItemProperty "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*" `
    -ErrorAction SilentlyContinue)

foreach ($entry in $regEntries) {
    try {
        $name = $entry.DisplayName
        if ($name -like "*BigFix*" -or $name -like "*BES Client*" -or $name -like "*Tivoli Endpoint*") {
            $bigfixReg = $entry
            break
        }
    } catch {
        # Some registry keys don't have DisplayName — skip silently
    }
}

if ($bigfixReg) {
    Write-Host "Found: $($bigfixReg.DisplayName) — uninstalling..." -ForegroundColor Yellow

    foreach ($svc in $serviceNames) {
        Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
    }

    try {
        # BESClientsetup.exe supports /uninstall /silent for clean removal
        $uninstallExe = Join-Path $installPath "BESClientsetup.exe"
        if (Test-Path $uninstallExe) {
            $proc = Start-Process $uninstallExe -ArgumentList "/uninstall /silent" -Wait -PassThru
            if ($proc.ExitCode -eq 0) {
                Write-Host "Uninstall complete." -ForegroundColor Green
            } else {
                Write-Warning "Uninstall returned code $($proc.ExitCode) — continuing anyway."
            }
        } elseif ($bigfixReg.UninstallString -match '\{[A-Z0-9\-]+\}') {
            # Fallback: MSI product code if present
            $productCode = $matches[0]
            $proc = Start-Process msiexec.exe -ArgumentList "/x $productCode /quiet /norestart" -Wait -PassThru
            Write-Host "MSI uninstall exit code: $($proc.ExitCode)" -ForegroundColor Green
        } else {
            Write-Warning "Could not locate uninstaller — skipping, continuing."
        }
    } catch {
        Write-Warning "Uninstall exception: $($_.Exception.Message) — continuing."
    }
} else {
    Write-Host "No existing BigFix Agent found. Fresh install." -ForegroundColor Cyan
}

# Remove leftover files and data
Remove-Item $downloadPath -Force -ErrorAction SilentlyContinue
Remove-Item $installPath  -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "C:\Program Files (x86)\BigFix Enterprise" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Install dir clean : $(-not (Test-Path $installPath))"

# ----------------------------
# 2. Download EXE from GCS
# ----------------------------
Write-Host "[2/6] Downloading BigFix Agent installer..." -ForegroundColor Yellow
gcloud storage cp $bucketPath $downloadPath

if (-not (Test-Path $downloadPath)) {
    Write-Warning "Installer download failed. Cannot continue."
    return
}

$fileSize = (Get-Item $downloadPath).Length
Write-Host "Download successful: $downloadPath" -ForegroundColor Green
Write-Host "File size: $([math]::Round($fileSize / 1MB, 1)) MB ($fileSize bytes)"
Unblock-File $downloadPath

# ----------------------------
# 3. Install Agent (unregistered — no masthead/relay specified)
# /S         = silent install
# /v"..."    = passes MSI-level properties through the EXE wrapper
# Omitting MSTRHOST / masthead defers registration to first boot
# ----------------------------
Write-Host "[3/6] Installing BigFix Agent (unregistered)..." -ForegroundColor Yellow
$installArgs = '/S /v"/qn /l*v \"{0}\""' -f $logPath
Start-Process $downloadPath -ArgumentList $installArgs -Wait -Verb RunAs
Write-Host "Installation complete. Log: $logPath" -ForegroundColor Green

# ----------------------------
# 4. Delete Computer ID — CRITICAL for image reusability
# Without this every VM from this image shares the same Computer ID,
# causing duplicate agent conflicts in the BigFix Root Server / Console.
# On first boot the agent registers fresh and gets a unique Computer ID.
# ----------------------------
Write-Host "[4/6] Deleting BigFix Computer ID for image reusability..." -ForegroundColor Yellow

$idPaths = @(
    "C:\Program Files (x86)\BigFix Enterprise\BES Client\__BESData\__Global\__RegParam",
    "C:\Program Files (x86)\BigFix Enterprise\BES Client\__BESData\__Global\ComputerID",
    "C:\Program Files\BigFix Enterprise\BES Client\__BESData\__Global\__RegParam",
    "C:\Program Files\BigFix Enterprise\BES Client\__BESData\__Global\ComputerID"
)

$idDeleted = $false
foreach ($idPath in $idPaths) {
    if (Test-Path $idPath) {
        Remove-Item $idPath -Recurse -Force
        Write-Host "  [OK] Deleted: $idPath" -ForegroundColor Green
        $idDeleted = $true
    }
}

# Clear registry-based Computer ID if written during install
$regPaths = @(
    "HKLM:\SOFTWARE\WOW6432Node\BigFix\EnterpriseClient\GlobalOptions",
    "HKLM:\SOFTWARE\BigFix\EnterpriseClient\GlobalOptions"
)
foreach ($regPath in $regPaths) {
    if (Test-Path $regPath) {
        Remove-ItemProperty -Path $regPath -Name "ComputerID" -ErrorAction SilentlyContinue
        Remove-ItemProperty -Path $regPath -Name "ComputerIDHigh" -ErrorAction SilentlyContinue
        Write-Host "  [OK] Cleared registry ComputerID at $regPath" -ForegroundColor Green
        $idDeleted = $true
    }
}

if (-not $idDeleted) {
    Write-Host "  [INFO] No Computer ID found yet — will be created fresh on first boot" -ForegroundColor Cyan
}

# ----------------------------
# 5. Verify binaries
# ----------------------------
Write-Host "[5/6] Verifying agent binaries..." -ForegroundColor Yellow

$agentBinPaths = @(
    "C:\Program Files (x86)\BigFix Enterprise\BES Client\BESClient.exe",
    "C:\Program Files\BigFix Enterprise\BES Client\BESClient.exe"
)

$agentFound = $false
foreach ($bin in $agentBinPaths) {
    if (Test-Path $bin) {
        Write-Host "  [OK] Agent binary: $bin" -ForegroundColor Green
        $agentFound = $true
        break
    }
}

if (-not $agentFound) {
    Write-Warning "  Agent binary not found — check install log: $logPath"
}

# ----------------------------
# 6. Stop BESClient service and set to Manual
# Client will start and register on first boot via masthead/relay
# ----------------------------
Write-Host "[6/6] Stopping BigFix services and setting to Manual..." -ForegroundColor Yellow

foreach ($svc in $serviceNames) {
    $s = Get-Service -Name $svc -ErrorAction SilentlyContinue
    if ($s) {
        Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
        Set-Service -Name $svc -StartupType Manual -ErrorAction SilentlyContinue
        Write-Host "  [OK] $svc — Stopped / Manual" -ForegroundColor Green
    } else {
        Write-Warning "  $svc service not found — check install log: $logPath"
    }
}

# Cleanup installer
Remove-Item $downloadPath -Force -ErrorAction SilentlyContinue
Write-Host "Installer cleaned up." -ForegroundColor Gray

# ----------------------------
# DONE
# ----------------------------
Write-Host "`n=== BigFix Agent installation complete ===" -ForegroundColor Green
Write-Host "Install path  : $installPath"                                    -ForegroundColor Cyan
Write-Host "Computer ID   : Deleted — fresh ID assigned on first boot"       -ForegroundColor Cyan
Write-Host "Services      : Stopped / Manual"                                -ForegroundColor Cyan
Write-Host "Install log   : $logPath"                                        -ForegroundColor Cyan
Write-Host "Registration  : Deferred — agent registers on first boot via Root Server" -ForegroundColor Cyan
