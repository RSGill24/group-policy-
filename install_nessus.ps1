# ==============================
# Nessus Agent Installation Script
# ==============================

$bucketPath    = "gs://nmfs-trusted-images-packages/nessus/NessusAgent-11.1.2-x64.msi"
$downloadPath  = "C:\Windows\Temp\NessusAgent-11.1.2-x64.msi"
$logPath       = "C:\Windows\Temp\nessus_install.log"
$installPath   = "C:\Program Files\Tenable\Nessus Agent"
$serviceName   = "Tenable Nessus Agent"

# ----------------------------
# 1. Uninstall existing
# FIX: Use try/catch loop instead of Where-Object with property access.
# StrictMode throws even inside Where-Object when property doesn't exist.
# ----------------------------
Write-Host "[1/5] Checking for existing Nessus Agent..." -ForegroundColor Yellow

$nessusReg = $null
$regEntries = Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*" `
    -ErrorAction SilentlyContinue

foreach ($entry in $regEntries) {
    try {
        $name = $entry.DisplayName
        if ($name -like "*Nessus Agent*") {
            $nessusReg = $entry
            break
        }
    } catch {
        # Some registry keys don't have DisplayName — skip silently
    }
}

if ($nessusReg) {
    Write-Host "Found: $($nessusReg.DisplayName) — uninstalling via msiexec..." -ForegroundColor Yellow
    Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue

    try {
        $uninstallStr = $nessusReg.UninstallString
        if ($uninstallStr -match '\{[A-Z0-9\-]+\}') {
            $productCode = $matches[0]
            $proc = Start-Process msiexec.exe -ArgumentList "/x $productCode /quiet /norestart" -Wait -PassThru
            if ($proc.ExitCode -eq 0) {
                Write-Host "Uninstall complete." -ForegroundColor Green
            } else {
                Write-Warning "Uninstall returned code $($proc.ExitCode) — continuing anyway."
            }
        } else {
            Write-Warning "Could not parse product code — skipping uninstall, continuing."
        }
    } catch {
        Write-Warning "Uninstall exception: $($_.Exception.Message) — continuing."
    }
} else {
    Write-Host "No existing Nessus Agent found. Fresh install." -ForegroundColor Cyan
}

Remove-Item $downloadPath                         -Force   -ErrorAction SilentlyContinue
Remove-Item $installPath                          -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "C:\ProgramData\Tenable\Nessus Agent" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Install dir clean : $(-not (Test-Path $installPath))"
Write-Host "Service gone      : $($null -eq (Get-Service -Name $serviceName -ErrorAction SilentlyContinue))"

# ----------------------------
# 2. Download MSI from GCS
# ----------------------------
Write-Host "[2/5] Downloading Nessus Agent MSI..." -ForegroundColor Yellow
gcloud storage cp $bucketPath $downloadPath

if (-not (Test-Path $downloadPath)) {
    Write-Warning "MSI download failed. Cannot continue."
    return
}

$fileSize = (Get-Item $downloadPath).Length
Write-Host "Download successful: $downloadPath" -ForegroundColor Green
Write-Host "File size: $([math]::Round($fileSize / 1MB, 1)) MB ($fileSize bytes)"
Unblock-File $downloadPath

# ----------------------------
# 3. Install MSI
# ----------------------------
Write-Host "[3/5] Installing Nessus Agent..." -ForegroundColor Yellow
$msiArgs = "/i `"$downloadPath`" /quiet /norestart /l*v `"$logPath`""
Start-Process msiexec.exe -ArgumentList $msiArgs -Wait -Verb RunAs
Write-Host "MSI installation complete. Log: $logPath" -ForegroundColor Green

# ----------------------------
# 4. Verify binaries
# ----------------------------
Write-Host "[4/5] Verifying agent binaries..." -ForegroundColor Yellow
$cliPath        = Join-Path $installPath "nessuscli.exe"
$serviceBinPath = Join-Path $installPath "nessus-service.exe"

Write-Host "nessuscli.exe exists:      $(Test-Path $cliPath)"
Write-Host "nessus-service.exe exists: $(Test-Path $serviceBinPath)"

# ----------------------------
# 5. Configure service — Stopped / Manual
# Client will start and link the agent on first boot
# ----------------------------
Write-Host "[5/5] Configuring Nessus service..." -ForegroundColor Yellow
$svc = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($svc) {
    Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
    Set-Service -Name $serviceName -StartupType Manual
    $startMode = (Get-WmiObject Win32_Service -Filter "Name='$serviceName'").StartMode
    Write-Host "Service: $($svc.DisplayName) | Stopped | StartupType: $startMode" -ForegroundColor Green
} else {
    Write-Warning "Service '$serviceName' not found — installation may have failed."
}

# Cleanup MSI
Remove-Item $downloadPath -Force -ErrorAction SilentlyContinue
Write-Host "MSI cleaned up." -ForegroundColor Gray

# ----------------------------
# DONE
# ----------------------------
$finalStatus    = (Get-Service -Name $serviceName -ErrorAction SilentlyContinue).Status
$finalStartMode = (Get-WmiObject Win32_Service -Filter "Name='$serviceName'").StartMode

Write-Host "`n=== Nessus Agent installation complete ===" -ForegroundColor Green
Write-Host "Install path : $installPath"                         -ForegroundColor Cyan
Write-Host "Data path    : C:\ProgramData\Tenable\Nessus Agent"  -ForegroundColor Cyan
Write-Host "Service      : $finalStatus | StartupType: $finalStartMode" -ForegroundColor Cyan
Write-Host "MSI log      : $logPath"                             -ForegroundColor Cyan
Write-Host "Activation   : Deferred — client will activate on first boot" -ForegroundColor Cyan
