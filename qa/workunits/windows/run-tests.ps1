$ProgressPreference = "SilentlyContinue"
$ErrorActionPreference = "Stop"

$scriptLocation = [System.IO.Path]::GetDirectoryName(
    $myInvocation.MyCommand.Definition)

$testRbdWnbd = "$scriptLocation/test_rbd_wnbd.py"

function safe_exec() {
    # Powershell doesn't check the command exit code, we'll need to
    # do it ourselves. Also, in case of native commands, it treats stderr
    # output as an exception, which is why we'll have to capture it.
    cmd /c "$args 2>&1"
    if ($LASTEXITCODE) {
        throw "Command failed: $args"
    }
}

safe_exec python.exe $testRbdWnbd --test-name RbdTest --iterations 100
safe_exec python.exe $testRbdWnbd --test-name RbdFioTest --iterations 100
safe_exec python.exe $testRbdWnbd --test-name RbdStampTest --iterations 100

# It can take a while to setup the partition (~10s), we'll use fewer iterations.
safe_exec python.exe $testRbdWnbd --test-name RbdFsTest --iterations 4
safe_exec python.exe $testRbdWnbd --test-name RbdFsFioTest --iterations 4
safe_exec python.exe $testRbdWnbd --test-name RbdFsStampTest --iterations 4

safe_exec python.exe $testRbdWnbd `
    --test-name RbdResizeFioTest --image-size-mb 64
