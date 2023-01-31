function DumpEventLog($path){
    foreach ($i in (Get-WinEvent -ListLog * |  ? {$_.RecordCount -gt 0 })) {
        $logName = "eventlog_" + $i.LogName + ".evtx"
        $logName = $logName.replace(" ","-").replace("/", "-").replace("\", "-")
        Write-Host "exporting "$i.LogName" as "$logName
        $logFile = Join-Path $path $logName
        & $Env:WinDir\System32\wevtutil.exe epl $i.LogName $logFile
    }
}

function ExportEventLog($path){
    foreach ($i in (Get-ChildItem $path -Filter eventlog_*.evtx)) {
        $logName = $i.BaseName + ".txt"
        $logName = $logName.replace(" ","-").replace("/", "-").replace("\", "-")
        Write-Host "converting "$i.BaseName" evtx to txt"
        $logFile = Join-Path $path $logName
        & $Env:WinDir\System32\wevtutil.exe qe $i.FullName /lf > $logFile
    }
}

function ClearEventLog(){
    foreach ($i in (Get-WinEvent -ListLog * |  ? {$_.RecordCount -gt 0 })) {
        & $Env:WinDir\System32\wevtutil.exe cl $i.LogName
    }
}


$logDir = $Env:WinDir + "\EventLogs"

mkdir -force $logDir

DumpEventLog $logDir
ExportEventLog $logDir
ClearEventLog

rm $logDir\eventlog_*.evtx
