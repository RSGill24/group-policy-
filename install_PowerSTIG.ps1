Write-Host "Installing PowerSTIG..."

# Enforce TLS 1.2 to ensure PSGallery HTTPS connections succeed on older .NET defaults
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# Trust PSGallery so Install-Module does not prompt for confirmation
if ((Get-PSRepository -Name PSGallery).InstallationPolicy -ne 'Trusted') {
    Set-PSRepository -Name PSGallery -InstallationPolicy Trusted
}

# Install NuGet provider if not already present — required by PowerShellGet / Install-Module
if (-not (Get-PackageProvider -Name NuGet -ErrorAction SilentlyContinue)) {
    Install-PackageProvider -Name NuGet -Force
}

# Install PowerSTIG for all users so it is available system-wide (e.g., SYSTEM scheduled task context)
# -AllowClobber : Overwrite any conflicting commands from other modules
Install-Module -Name PowerSTIG -Scope AllUsers -Force -AllowClobber

# Import into the current session so subsequent scripts can use it immediately
Import-Module PowerSTIG -Force

Write-Host "PowerSTIG installed successfully."
