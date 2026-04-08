# ==============================
# Trellix HX (xagt) Agent Installation Script - FINAL
# ==============================

$bucketMSI    = "gs://nmfs-trusted-images-packages/fireeye/xagtSetup_36.30.17_universal (1).msi"
$bucketConfig = "gs://org-sec-agents-bucket/agent_config.json"          # Updated filename
$msiDir       = "C:\Windows\Temp\xagt_install"
$downloadMSI  = "$msiDir\xagtSetup_36.30.17_universal.msi"
$downloadCfg  = "$msiDir\agent_config.json"
$logPath      = "C:\Windows\Temp\xagt_install.log"
$debugLog     = "C:\Windows\Temp\xagt_debug.log"
$serviceNames = @("xagt")

function Log($msg, $color = "White") {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $msg"
    Write-Host $line -ForegroundColor $color
    Add-Content -Path $debugLog -Value $line
}

if (Test-Path $debugLog) { Remove-Item $debugLog -Force }
Log "===== Trellix HX Agent Install — FINAL =====" "Cyan"
Log "Running as : $($env:USERNAME)"
Log "Computer   : $($env:COMPUTERNAME)"
Log "OS         : $([System.Environment]::OSVersion.VersionString)"

# ----------------------------
# 1. Uninstall existing
# ----------------------------
Log "[1/7] Checking for existing Trellix HX Agent..." "Yellow"

$xagtReg = $null
$regEntries  = @(Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*" -ErrorAction SilentlyContinue)
$regEntries += @(Get-ItemProperty "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*" -ErrorAction SilentlyContinue)

foreach ($entry in $regEntries) {
    try {
        if ($entry.DisplayName -like "*xagt*" -or $entry.DisplayName -like "*FireEye*" -or $entry.DisplayName -like "*Trellix*") {
            $xagtReg = $entry; break
        }
    } catch { }
}

if ($xagtReg) {
    Log "Found existing: $($xagtReg.DisplayName)" "Yellow"
    foreach ($svc in $serviceNames) {
        Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
        Log "  Stopped service: $svc"
    }
    try {
        if ($xagtReg.UninstallString -match '\{[A-Z0-9\-]+\}') {
            $productCode = $matches[0]
            Log "  Uninstalling: $productCode"
            $proc = Start-Process msiexec.exe -ArgumentList "/x $productCode /quiet /norestart" -Wait -PassThru
            Log "  Uninstall exit code: $($proc.ExitCode)"
        } else {
            Log "  WARNING: Could not parse product code — skipping uninstall" "Red"
        }
    } catch {
        Log "  WARNING: Uninstall exception: $($_.Exception.Message)" "Red"
    }
} else {
    Log "No existing agent found — fresh install" "Cyan"
}

# ----------------------------
# 2. Full cleanup
# ----------------------------
Log "[2/7] Cleaning up leftover directories..." "Yellow"

$dirsToClean = @(
    "C:\ProgramData\FireEye",
    "C:\Program Files\FireEye",
    "C:\Program Files (x86)\FireEye",
    "C:\Windows\FireEye",
    $msiDir
)
foreach ($dir in $dirsToClean) {
    Remove-Item $dir -Recurse -Force -ErrorAction SilentlyContinue
    Log "  Cleaned: $dir"
}

# ----------------------------
# 3. Pre-create dirs with full permissions
# ----------------------------
Log "[3/7] Pre-creating required directories..." "Yellow"

$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "Everyone","FullControl","ContainerInherit,ObjectInherit","None","Allow"
)

foreach ($dir in @($msiDir, "C:\Windows\FireEye", "C:\ProgramData\FireEye")) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    $acl = Get-Acl $dir
    $acl.SetAccessRule($rule)
    Set-Acl $dir $acl
    Log "  Created + permissioned: $dir" "Green"
}

# ----------------------------
# 4. Download MSI + agent_config.json
# ----------------------------
Log "[4/7] Downloading from GCS..." "Yellow"

gcloud storage cp $bucketMSI $downloadMSI
Log "  MSI download exit code: $LASTEXITCODE"
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $downloadMSI)) {
    Log "FATAL: MSI download failed — cannot continue" "Red"; return
}

gcloud storage cp $bucketConfig $downloadCfg
Log "  Config download exit code: $LASTEXITCODE"
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $downloadCfg)) {
    Log "FATAL: config download failed — cannot continue" "Red"; return
}

$msiSize = (Get-Item $downloadMSI).Length
$cfgSize = (Get-Item $downloadCfg).Length
Log "  MSI size    : $([math]::Round($msiSize/1MB,1)) MB" "Green"
Log "  Config size : $cfgSize bytes" "Green"

Unblock-File $downloadMSI
Unblock-File $downloadCfg

# Pre-place agent_config.json in ProgramData (required by Action.ImportConfig)
Copy-Item $downloadCfg "C:\ProgramData\FireEye\agent_config.json" -Force
Log "  agent_config.json next to MSI    : $(Test-Path $downloadCfg)" "Green"
Log "  agent_config.json in ProgramData : $(Test-Path 'C:\ProgramData\FireEye\agent_config.json')" "Green"

# ----------------------------
# 5. Install MSI
# ----------------------------
Log "[5/7] Installing Trellix HX Agent..." "Yellow"

$msiArgs = "/i `"$downloadMSI`" /quiet /norestart /l*v `"$logPath`""
Log "  Args: $msiArgs"
$proc = Start-Process msiexec.exe -ArgumentList $msiArgs -Wait -PassThru -Verb RunAs
$exitCode = $proc.ExitCode
Log "  Exit code: $exitCode"

switch ($exitCode) {
    0     { Log "  MSI install SUCCESS" "Green" }
    3010  { Log "  MSI install SUCCESS — reboot required" "Yellow" }
    1603  { Log "  ERROR 1603: Fatal install error — check $logPath" "Red" }
    1618  { Log "  ERROR 1618: Another MSI already running" "Red" }
    1619  { Log "  ERROR 1619: MSI cannot be opened — corrupt?" "Red" }
    1638  { Log "  ERROR 1638: Another version already installed" "Red" }
    default { Log "  ERROR: Unexpected exit code $exitCode" "Red" }
}

if ($exitCode -notin @(0, 3010)) {
    if (Test-Path $logPath) {
        $msiErrors = Get-Content $logPath | Select-String -Pattern "error|fail|return value [23]|actions.dll" -CaseSensitive:$false | Select-Object -Last 20
        foreach ($line in $msiErrors) { Log "  MSI: $line" "Red" }
    }
    Log "FATAL: Install failed — stopping" "Red"
    return
}

# ----------------------------
# 6. Delete Agent UUID — CRITICAL for image reusability
# ----------------------------
Log "[6/7] Removing agent.uuid for image reusability..." "Yellow"

$uuidPaths = @(
    "C:\ProgramData\FireEye\xagt\agent.uuid",
    "C:\ProgramData\FireEye\xagt\uuid",
    "C:\Program Files (x86)\FireEye\xagt\agent.uuid"
)
foreach ($uuidPath in $uuidPaths) {
    if (Test-Path $uuidPath) {
        Remove-Item $uuidPath -Force
        Log "  Deleted UUID: $uuidPath" "Green"
    }
}
Log "  UUID removed — fresh UUID generated on first boot" "Green"

# ----------------------------
# 7. Stop service + set Manual
# ----------------------------
Log "[7/7] Stopping xagt service and setting to Manual..." "Yellow"

foreach ($svc in $serviceNames) {
    $s = Get-Service -Name $svc -ErrorAction SilentlyContinue
    if ($s) {
        Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
        Set-Service -Name $svc -StartupType Manual -ErrorAction SilentlyContinue
        Log "  $svc — Stopped / Manual" "Green"
    } else {
        Log "  WARNING: $svc service not found" "Red"
    }
}

# Cleanup temp files
Remove-Item $msiDir -Recurse -Force -ErrorAction SilentlyContinue
Log "Temp files cleaned"

# ----------------------------
# DONE
# ----------------------------
Log "`n===== Trellix HX Agent install complete =====" "Green"
Log "Install path : C:\Program Files (x86)\FireEye\xagt"        "Cyan"
Log "Config path  : C:\ProgramData\FireEye\agent_config.json"   "Cyan"
Log "Agent UUID   : Deleted — fresh UUID on first boot"         "Cyan"
Log "Service      : Stopped / Manual"                           "Cyan"
Log "MSI log      : $logPath"                                   "Cyan"
Log "Debug log    : $debugLog"                                  "Cyan"
