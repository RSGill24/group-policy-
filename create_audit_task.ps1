# Determine the script directory so task keeps working with any hardening_target_dir
$scriptDir = $PSScriptRoot
$ps1FilePath = Join-Path $scriptDir 'run_only_audit.ps1'

# Quote the file path for Scheduled Task argument safety
$actionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$ps1FilePath`""
$Action = New-ScheduledTaskAction -Execute "Powershell.exe" -Argument $actionArgs

# Define the trigger for every 2 weeks
$BiWeeklyTrigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 2 -DaysOfWeek Wednesday -At 9am

# If biweekly works, this may be overkill. test both for now.
$LogonTrigger = New-ScheduledTaskTrigger -AtLogOn

# Define task settings to allow execution if missed
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries -DontStopOnIdleEnd -MultipleInstances Parallel

# Register the task to run as SYSTEM with both triggers
Register-ScheduledTask -TaskName "Push_DISA_STIG_Audit" -Action $Action -Trigger $BiWeeklyTrigger, $LogonTrigger -Settings $Settings -User "SYSTEM" -RunLevel Highest
