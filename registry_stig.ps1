Write-Host "=== Applying Registry STIG (Targeted Fix) ==="

# V-254343.b
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\Lsa" `
  -Name "LmCompatibilityLevel" -Value 1 -Type DWord -Force

# V-254344
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\Lsa" `
  -Name "NoLMHash" -Value 1 -Type DWord -Force

# V-254357
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management" `
  -Name "ClearPageFileAtShutdown" -Value 1 -Type DWord -Force

# Event Logs (V-254358 / 359 / 360)
wevtutil sl Security /ms:196608
wevtutil sl Application /ms:32768
wevtutil sl System /ms:32768

# V-254432
Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" `
  -Name "ConsentPromptBehaviorAdmin" -Value 4 -Type DWord -Force

# V-254454 (Idle timeout)
Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" `
  -Name "InactivityTimeoutSecs" -Value 900 -Type DWord -Force

Write-Host "=== Registry STIG Fixed ==="
