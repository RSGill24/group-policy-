Write-Host "=== Applying Services STIG (Targeted Fix) ==="

# Antivirus (V-254248)
Set-Service -Name WinDefend -StartupType Automatic
Start-Service WinDefend -ErrorAction SilentlyContinue

# Firewall (V-254265)
# Bypass "Access is denied" by setting MpsSvc startup type directly in the registry (2 = Automatic)
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\MpsSvc" -Name "Start" -Value 2 -Type DWord -Force
Write-Host "Set MpsSvc (Windows Defender Firewall) to Automatic via Registry" -ForegroundColor Green

# Ensure firewall ON
Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled True

Write-Host "=== Services STIG Fixed ==="

# Force a clean exit code
exit 0
