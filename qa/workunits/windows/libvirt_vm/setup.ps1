$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$PYTHON3_URL = "https://www.python.org/ftp/python/3.10.4/python-3.10.4-amd64.exe"
$FIO_URL = "https://bsdio.com/fio/releases/fio-3.27-x64.msi"
$VC_REDIST_URL = "https://aka.ms/vs/17/release/vc_redist.x64.exe"

. "${PSScriptRoot}\utils.ps1"

function Install-VCRedist {
    Write-Output "Installing Visual Studio Redistributable x64"
    Install-Tool -URL $VC_REDIST_URL -Params @("/quiet", "/norestart")
    Write-Output "Successfully installed Visual Studio Redistributable x64"
}

function Install-Python3 {
    Write-Output "Installing Python3"
    Install-Tool -URL $PYTHON3_URL -Params @("/quiet", "InstallAllUsers=1", "PrependPath=1")
    Add-ToPathEnvVar -Path @("${env:ProgramFiles}\Python310\", "${env:ProgramFiles}\Python310\Scripts\")
    Write-Output "Installing pip dependencies"
    Start-ExecuteWithRetry {
        Invoke-CommandLine "pip3.exe" "install prettytable"
    }
    Write-Output "Successfully installed Python3"
}

function Install-FIO {
    Write-Output "Installing FIO"
    Install-Tool -URL $FIO_URL -Params @("/qn", "/l*v", "$env:TEMP\fio-install.log", "/norestart")
    Write-Output "Successfully installed FIO"
}

Install-VCRedist
Install-Python3
Install-FIO

# Pre-append WNBD and Ceph to PATH
Add-ToPathEnvVar -Path @(
    "${env:SystemDrive}\wnbd\binaries",
    "${env:SystemDrive}\ceph")

# This will refresh the PATH for new SSH sessions
Restart-Service -Force -Name "sshd"
