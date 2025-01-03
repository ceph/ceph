function Invoke-CommandLine {
    Param(
        [Parameter(Mandatory=$true)]
        [String]$Command,
        [String]$Arguments,
        [Int[]]$AllowedExitCodes=@(0)
    )
    & $Command $Arguments.Split(" ")
    if($LASTEXITCODE -notin $AllowedExitCodes) {
        Throw "$Command $Arguments returned a non zero exit code ${LASTEXITCODE}."
    }
}

function Start-ExecuteWithRetry {
    Param(
        [Parameter(Mandatory=$true)]
        [ScriptBlock]$ScriptBlock,
        [Int]$MaxRetryCount=10,
        [Int]$RetryInterval=3,
        [String]$RetryMessage,
        [Array]$ArgumentList=@()
    )
    $currentErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $retryCount = 0
    while ($true) {
        try {
            $res = Invoke-Command -ScriptBlock $ScriptBlock -ArgumentList $ArgumentList
            $ErrorActionPreference = $currentErrorActionPreference
            return $res
        } catch [System.Exception] {
            $retryCount++
            if ($retryCount -gt $MaxRetryCount) {
                $ErrorActionPreference = $currentErrorActionPreference
                Throw $_
            } else {
                $prefixMsg = "Retry(${retryCount}/${MaxRetryCount})"
                if($RetryMessage) {
                    Write-Host "${prefixMsg} - $RetryMessage"
                } elseif($_) {
                    Write-Host "${prefixMsg} - $($_.ToString())"
                }
                Start-Sleep $RetryInterval
            }
        }
    }
}

function Start-FileDownload {
    Param(
        [Parameter(Mandatory=$true)]
        [String]$URL,
        [Parameter(Mandatory=$true)]
        [String]$Destination,
        [Int]$RetryCount=10
    )
    Write-Output "Downloading $URL to $Destination"
    Start-ExecuteWithRetry `
        -ScriptBlock { Invoke-CommandLine -Command "curl.exe" -Arguments "-L -s -o $Destination $URL" } `
        -MaxRetryCount $RetryCount `
        -RetryMessage "Failed to download '${URL}'. Retrying"
    Write-Output "Successfully downloaded."
}

function Add-ToPathEnvVar {
    Param(
        [Parameter(Mandatory=$true)]
        [String[]]$Path,
        [Parameter(Mandatory=$false)]
        [ValidateSet([System.EnvironmentVariableTarget]::User, [System.EnvironmentVariableTarget]::Machine)]
        [System.EnvironmentVariableTarget]$Target=[System.EnvironmentVariableTarget]::Machine
    )
    $pathEnvVar = [Environment]::GetEnvironmentVariable("PATH", $Target).Split(';')
    $currentSessionPath = $env:PATH.Split(';')
    foreach($p in $Path) {
        if($p -notin $pathEnvVar) {
            $pathEnvVar += $p
        }
        if($p -notin $currentSessionPath) {
            $currentSessionPath += $p
        }
    }
    $env:PATH = $currentSessionPath -join ';'
    $newPathEnvVar = $pathEnvVar -join ';'
    [Environment]::SetEnvironmentVariable("PATH", $newPathEnvVar, $Target)
}

function Install-Tool {
    [CmdletBinding(DefaultParameterSetName = "URL")]
    Param(
        [Parameter(Mandatory=$true, ParameterSetName = "URL")]
        [String]$URL,
        [Parameter(Mandatory=$true, ParameterSetName = "LocalPath")]
        [String]$LocalPath,
        [Parameter(ParameterSetName = "URL")]
        [Parameter(ParameterSetName = "LocalPath")]
        [String[]]$Params=@(),
        [Parameter(ParameterSetName = "URL")]
        [Parameter(ParameterSetName = "LocalPath")]
        [Int[]]$AllowedExitCodes=@(0)
    )
    PROCESS {
        $installerPath = $LocalPath
        if($PSCmdlet.ParameterSetName -eq "URL") {
            $installerPath = Join-Path $env:TEMP $URL.Split('/')[-1]
            Start-FileDownload -URL $URL -Destination $installerPath
        }
        Write-Output "Installing ${installerPath}"
        $kwargs = @{
            "FilePath" = $installerPath
            "ArgumentList" = $Params
            "NoNewWindow" = $true
            "PassThru" = $true
            "Wait" = $true
        }
        if((Get-ChildItem $installerPath).Extension -eq '.msi') {
            $kwargs["FilePath"] = "msiexec.exe"
            $kwargs["ArgumentList"] = @("/i", $installerPath) + $Params
        }
        $p = Start-Process @kwargs
        if($p.ExitCode -notin $AllowedExitCodes) {
            Throw "Installation failed. Exit code: $($p.ExitCode)"
        }
        if($PSCmdlet.ParameterSetName -eq "URL") {
            Start-ExecuteWithRetry `
                -ScriptBlock { Remove-Item -Force -Path $installerPath -ErrorAction Stop } `
                -RetryMessage "Failed to remove ${installerPath}. Retrying"
        }
    }
}
