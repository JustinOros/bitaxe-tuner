[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$MonitorArgs
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

$Root = $PSScriptRoot
if (-not $Root) { $Root = (Get-Location).Path }
$Script = Join-Path $Root "bitaxe-monitor.py"
$TunerScript = Join-Path $Root "bitaxe-tuner.py"
$PortableDir = Join-Path $Root ".python"
$PortableExe = Join-Path $PortableDir "python.exe"
$PythonVersion = "3.12.8"
$ScriptUrl = "https://raw.githubusercontent.com/JustinOros/bitaxe-tuner/main/bitaxe-monitor.py"
$TunerUrl = "https://raw.githubusercontent.com/JustinOros/bitaxe-tuner/main/bitaxe-tuner.py"

function Ask([string]$Question, [string]$Default = "n") {
    $hint = if ($Default -eq "y") { "[Y/n]" } else { "[y/N]" }
    $a = (Read-Host "$Question $hint").Trim().ToLower()
    if (-not $a) { $a = $Default }
    return $a -eq "y"
}

function Test-Python([string]$Exe, [string[]]$PreArgs = @()) {
    try {
        $out = & $Exe @PreArgs -c "import sys; print(sys.version_info[0]*100+sys.version_info[1])" 2>$null
        return ([int]("$out".Trim()) -ge 308)
    } catch {
        return $false
    }
}

function Find-Python {
    if (Test-Path $PortableExe) {
        if (Test-Python $PortableExe) { return @{ Exe = $PortableExe; Pre = @() } }
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py -and (Test-Python $py.Source @("-3"))) { return @{ Exe = $py.Source; Pre = @("-3") } }
    foreach ($name in "python", "python3") {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($cmd -and $cmd.Source -notlike "*WindowsApps*" -and (Test-Python $cmd.Source)) {
            return @{ Exe = $cmd.Source; Pre = @() }
        }
    }
    return $null
}

function Install-Portable {
    $arch = $env:PROCESSOR_ARCHITECTURE
    $tag = switch ($arch) {
        "ARM64" { "arm64" }
        "x86" { "win32" }
        default { "amd64" }
    }
    $url = "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-embed-$tag.zip"
    $zip = Join-Path $env:TEMP "bitaxe-python-$PythonVersion.zip"
    Write-Host "downloading Python $PythonVersion ($tag) from python.org"
    Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
    if (Test-Path $PortableDir) { Remove-Item $PortableDir -Recurse -Force }
    New-Item -ItemType Directory -Path $PortableDir | Out-Null
    Expand-Archive -Path $zip -DestinationPath $PortableDir -Force
    Remove-Item $zip -Force
    if (-not (Test-Path $PortableExe)) { throw "Python did not unpack correctly" }
    Write-Host "Python is ready in $PortableDir"
}

function Install-WithWinget {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) { throw "winget was not found on this PC" }
    & winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) { throw "winget could not install Python" }
    $env:PATH = [Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [Environment]::GetEnvironmentVariable("PATH", "User")
}

function Ensure-Python {
    $found = Find-Python
    if ($found) { return $found }
    Write-Host "Python 3.8 or newer was not found"
    Write-Host "  1) portable copy inside this folder (no admin needed, easy to delete)"
    Write-Host "  2) install Python system wide with winget"
    Write-Host "  3) cancel"
    $choice = (Read-Host "choose [1]").Trim()
    if (-not $choice) { $choice = "1" }
    switch ($choice) {
        "1" { Install-Portable }
        "2" { Install-WithWinget }
        default { Write-Host "cancelled"; exit 1 }
    }
    $found = Find-Python
    if (-not $found) { throw "Python still was not found after installing" }
    return $found
}

function Ensure-Script {
    if (-not (Test-Path $Script)) {
        Write-Host "bitaxe-monitor.py was not found next to this script"
        if (-not (Ask "download it from GitHub now?" "y")) { exit 1 }
        Invoke-WebRequest -Uri $ScriptUrl -OutFile $Script -UseBasicParsing
        if (-not (Test-Path $Script)) { throw "download failed" }
    }
    if (-not (Test-Path $TunerScript)) {
        Write-Host "bitaxe-tuner.py was not found, the Tuner tab needs it"
        if (Ask "download it from GitHub now?" "y") {
            Invoke-WebRequest -Uri $TunerUrl -OutFile $TunerScript -UseBasicParsing
        }
    }
}

Add-Type -Namespace Bitaxe -Name Power -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("kernel32.dll")]
public static extern uint SetThreadExecutionState(uint flags);
'@

try {
    Ensure-Script
    $python = Ensure-Python
    $env:PYTHONUTF8 = "1"
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONUNBUFFERED = "1"
    [void][Bitaxe.Power]::SetThreadExecutionState([uint32]2147483649)
    Push-Location $Root
    try {
        $pre = $python.Pre
        & $python.Exe @pre $Script @TunerArgs
        $code = $LASTEXITCODE
    } finally {
        Pop-Location
        [void][Bitaxe.Power]::SetThreadExecutionState([uint32]2147483648)
    }
    exit $code
} catch {
    Write-Host ("error: " + $_.Exception.Message) -ForegroundColor Red
    exit 1
}
