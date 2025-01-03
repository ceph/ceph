$ErrorActionPreference = "Stop"

. "${PSScriptRoot}\utils.ps1"

$VIRTIO_WIN_PATH = "E:\"

# Install QEMU quest agent
Write-Output "Installing QEMU guest agent"
$p = Start-Process -FilePath "msiexec.exe" -ArgumentList @("/i", "${VIRTIO_WIN_PATH}\guest-agent\qemu-ga-x86_64.msi", "/qn") -NoNewWindow -PassThru -Wait
if($p.ExitCode) {
    Throw "The QEMU guest agent installation failed. Exit code: $($p.ExitCode)"
}
Write-Output "Successfully installed QEMU guest agent"

# Install OpenSSH server
Start-ExecuteWithRetry {
    Get-WindowsCapability -Online -Name OpenSSH* | Add-WindowsCapability -Online
}

# Start OpenSSH server
Set-Service -Name "sshd" -StartupType Automatic
Start-Service -Name "sshd"

# Set PowerShell as default SSH shell
New-ItemProperty -PropertyType String -Force -Name DefaultShell -Path "HKLM:\SOFTWARE\OpenSSH" -Value (Get-Command powershell.exe).Source

# Create SSH firewall rule
New-NetFirewallRule -Name "sshd" -DisplayName 'OpenSSH Server (sshd)' -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22

# Authorize the SSH key
$authorizedKeysFile = Join-Path $env:ProgramData "ssh\administrators_authorized_keys"
Set-Content -Path $authorizedKeysFile -Value (Get-Content "${PSScriptRoot}\id_rsa.pub") -Encoding ascii
$acl = Get-Acl $authorizedKeysFile
$acl.SetAccessRuleProtection($true, $false)
$administratorsRule = New-Object system.security.accesscontrol.filesystemaccessrule("Administrators", "FullControl", "Allow")
$systemRule = New-Object system.security.accesscontrol.filesystemaccessrule("SYSTEM", "FullControl", "Allow")
$acl.SetAccessRule($administratorsRule)
$acl.SetAccessRule($systemRule)
$acl | Set-Acl

# Reboot the machine to complete first logon process
Restart-Computer -Force -Confirm:$false
