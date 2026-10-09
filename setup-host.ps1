# Native Windows PowerShell 5.1+ / PowerShell 7. No Python/pip/venv bootstrap.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0
$mode = ''
$profile = 'dev'
$deferVcpkg = $false
$PSNativeCommandUseErrorActionPreference = $false
$toolsDir = Join-Path $PSScriptRoot 'out/host-tools'
$userDirectory = if ($env:USERPROFILE) { $env:USERPROFILE } else { $HOME }
$vcpkgRoot = if ($env:VCPKG_ROOT) { $env:VCPKG_ROOT } else { Join-Path $userDirectory 'vcpkg' }
$setupArgs = @($args)
for ($i = 0; $i -lt $setupArgs.Count; $i++) {
    $argument = $setupArgs[$i]
    # Accept native PowerShell spelling and the existing GNU-style arguments.
    $aliases = @{ '-Install'='--install'; '-Check'='--check'; '-DryRun'='--dry-run';
        '-Profile'='--profile'; '-ToolsDir'='--tools-dir';
        '-VcpkgRoot'='--vcpkg-root'; '-DeferVcpkg'='--defer-vcpkg'; '-Help'='--help' }
    if ($aliases.ContainsKey($argument)) { $argument = $aliases[$argument] }
    switch ($argument) {
        '--defer-vcpkg' { $deferVcpkg = $true }
        { $_ -in '--install', '--check', '--dry-run' } {
            if ($mode) { throw 'Choose exactly one mode.' }
            $mode = $argument
        }
        { $_ -in '--tools-dir', '--vcpkg-root', '--profile' } {
            if ($i + 1 -ge $setupArgs.Count -or -not $setupArgs[$i + 1] -or $setupArgs[$i + 1] -like '--*') {
                throw "Missing value for $argument"
            }
            $i++
            if ($argument -eq '--profile') { $profile = $setupArgs[$i] } elseif ($argument -eq '--tools-dir') { $toolsDir = $setupArgs[$i] } else { $vcpkgRoot = $setupArgs[$i] }
        }
        { $_ -in '--help', '-h' } {
            Write-Host 'Usage: .\setup-host.ps1 --install|--check|--dry-run [--tools-dir PATH] [--vcpkg-root PATH]'
            Write-Host 'Native syntax: ./setup-host.ps1 -Install|-Check|-DryRun -Profile minimal|package|dev|ci (default: dev)'
            Write-Host '--defer-vcpkg reuses an existing SDK, or lets CMake provision it when needed.'
            Write-Host 'The harness runs in CMake; setup does not require Python.'
            exit 0
        }
        default { throw "Unknown argument: $argument" }
    }
}
if (-not $mode) { throw 'Choose --install, --check or --dry-run. See --help.' }
if ($profile -notin 'minimal', 'package', 'dev', 'ci') { throw "Unknown profile: $profile" }
$isWindowsHost = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
if (-not $isWindowsHost -and $mode -ne '--dry-run') { throw 'Use setup-host.sh on Linux/macOS.' }
Write-Host "Host: Windows; manager: winget; profile: $profile; mode: $mode"
$toolsDir = [IO.Path]::GetFullPath($toolsDir)
$vcpkgRoot = [IO.Path]::GetFullPath($vcpkgRoot)
$overlayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'vcpkg'))
if ($vcpkgRoot.TrimEnd('\', '/') -eq $overlayRoot.TrimEnd('\', '/')) { throw 'vcpkg/ holds overlays; choose ~/vcpkg or external/vcpkg.' }
if ((Test-Path $vcpkgRoot) -and -not (Test-Path (Join-Path $vcpkgRoot 'scripts/buildsystems/vcpkg.cmake'))) {
    throw "$vcpkgRoot exists but is not a vcpkg checkout."
}
$programFilesPath = if ($env:ProgramFiles) { $env:ProgramFiles } else { 'C:\Program Files' }
$installerBase = if (${env:ProgramFiles(x86)}) { ${env:ProgramFiles(x86)} } else { 'C:\Program Files (x86)' }
function Find-VisualStudio([switch]$AnyWorkload) {
    if (-not $isWindowsHost) { return '' }
    $finder = Join-Path $installerBase 'Microsoft Visual Studio/Installer/vswhere.exe'
    if (-not (Test-Path $finder)) { return '' }
    $arguments = @('-latest', '-products', '*', '-property', 'installationPath')
    if (-not $AnyWorkload) { $arguments += @('-requires', 'Microsoft.VisualStudio.Component.VC.Tools.x86.x64') }
    $result = & $finder @arguments
    if ($LASTEXITCODE -ne 0) { throw 'vswhere failed.' }
    return ($result -join '').Trim()
}
function Refresh-ToolPath {
    $paths = @($vcpkgRoot)
    if ($isWindowsHost) {
        foreach ($scope in 'Machine', 'User') {
            $storedPath = [Environment]::GetEnvironmentVariable('Path', $scope)
            if ($storedPath) { $paths += [Environment]::ExpandEnvironmentVariables($storedPath).Split(';') }
        }
        foreach ($relative in 'LLVM/bin', 'Git/cmd', 'CMake/bin', 'Cppcheck', 'doxygen/bin', 'NSIS') {
            $paths += Join-Path $programFilesPath $relative
        }
        # The official NSIS installer defaults to Program Files (x86).
        $paths += Join-Path $installerBase 'NSIS'
        $paths += @(Get-ChildItem (Join-Path $programFilesPath 'Graphviz*/bin') -Directory -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName })
        $paths += @(Get-ChildItem "$env:SystemDrive/VulkanSDK/*/Bin" -Directory -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | ForEach-Object { $_.FullName })
    }
    $script:activationPaths = @($paths | Where-Object { $_ } | Select-Object -Unique)
    $env:PATH = ($activationPaths -join [IO.Path]::PathSeparator) + [IO.Path]::PathSeparator + $env:PATH
}
function Invoke-Tool([string]$Executable, [string[]]$Arguments) {
    Write-Host ('+ ' + $Executable + ' ' + (($Arguments | ForEach-Object { "'" + $_.Replace("'", "''") + "'" }) -join ' '))
    if ($mode -ne '--dry-run') {
        & $Executable @Arguments
        if ($LASTEXITCODE -ne 0) { throw "$Executable failed with exit code $LASTEXITCODE" }
    }
}
function Find-Tool([string]$Name) {
    if ($Name -eq 'clang++') { $names = @('clang++', 'clang-cl') }
    elseif ($Name -eq 'ld.lld') { $names = @('ld.lld', 'lld-link') }
    else { $names = @($Name) }
    return Get-Command $names -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
}
$tools = @(Get-Content (Join-Path $PSScriptRoot 'scripts/host-tools.txt') |
    Where-Object { $_ -and -not $_.StartsWith('#') } |
    ConvertFrom-Csv -Delimiter '|' -Header Label, Command, Apt, Dnf, Pacman, Brew, Winget, Profiles | Where-Object { $profile -in $_.Profiles.Split(',') })
function Test-ToolReady($Tool) {
    if (-not (Find-Tool $Tool.Command)) { return $false }
    if ($Tool.Command -eq 'cmake') {
        $versionOutput = & cmake --version
        if ($LASTEXITCODE -ne 0 -or ($versionOutput -join ' ') -notmatch 'version (\d+\.\d+\.\d+)') { return $false }
        return ([version]$Matches[1] -ge [version]'3.26.0')
    }
    return $true
}
$initialReady = @{}
$reportPhase = 'before'
function Write-ToolStatus([string]$Label, [bool]$Ready) {
    if ($Ready) {
        $status = 'ALREADY'
        if ($reportPhase -eq 'after' -and -not $initialReady.ContainsKey($Label)) { $status = 'INSTALLED' }
        $initialReady[$Label] = $true
    } else { $status = 'MISSING' }
    Write-Host ('{0,-10} {1}' -f $status, $Label)
    return $status
}
Refresh-ToolPath
function Test-HostTools {
    $counts = @{ ALREADY=0; INSTALLED=0; MISSING=0 }
    foreach ($tool in $tools) {
        if ($tool.Label -eq 'compiler') {
            $status = Write-ToolStatus 'compiler' ([bool](Find-VisualStudio))
            $counts[$status]++
            continue
        }
        if ($tool.Winget -eq '-') { Write-Host "N/A       $($tool.Label)"; continue }
        $status = Write-ToolStatus $tool.Label (Test-ToolReady $tool)
        $counts[$status]++
    }
    if ($deferVcpkg -and -not (Test-Path (Join-Path $vcpkgRoot 'vcpkg.exe'))) {
        Write-Host 'DEFERRED  vcpkg: CMake provisions the pinned SDK/executable when needed.'
    } else {
        $status = Write-ToolStatus 'vcpkg' ((Test-Path (Join-Path $vcpkgRoot 'vcpkg.exe')) -and (Test-Path (Join-Path $vcpkgRoot 'scripts/buildsystems/vcpkg.cmake')))
        $counts[$status]++
    }
    Write-Host "Summary: already=$($counts.ALREADY) installed=$($counts.INSTALLED) missing=$($counts.MISSING)"
    Write-Host 'SEPARATE  CUDA, DXC and Emscripten: install separately.'
    return ($counts.MISSING -eq 0)
}
if ($mode -eq '--check') { if (Test-HostTools) { exit 0 } else { exit 1 } }
$null = Test-HostTools
$packages = @($tools | Where-Object { $_.Winget -ne '-' -and -not (Test-ToolReady $_) } |
    ForEach-Object { $_.Winget } | Select-Object -Unique)
$failed = $false
# Windows Server CI images can have Chocolatey but no supported WinGet. Use
# either native package manager without requiring the user to provision one.
$useChoco = $isWindowsHost -and -not (Get-Command winget -ErrorAction SilentlyContinue) -and
    [bool](Get-Command choco -ErrorAction SilentlyContinue)
$chocoIds = @{
    'Git.Git' = 'git'; 'Kitware.CMake' = 'cmake';
    'Ninja-build.Ninja' = 'ninja'; 'NSIS.NSIS' = 'nsis'
}
if ($mode -eq '--install' -and ($packages.Count -gt 0 -or -not (Find-VisualStudio)) -and -not $useChoco -and -not (Get-Command winget -ErrorAction SilentlyContinue)) {
    Write-Host 'Installing WinGet through Microsoft.WinGet.Client...'
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    Install-PackageProvider -Name NuGet -Scope CurrentUser -Force | Out-Null
    Install-Module -Name Microsoft.WinGet.Client -Scope CurrentUser -Force -Repository PSGallery
    Import-Module Microsoft.WinGet.Client
    Repair-WinGetPackageManager -Force -Latest
    Refresh-ToolPath
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) { throw 'WinGet bootstrap failed.' }
}
foreach ($package in $packages) {
    try {
        if ($useChoco) {
            if (-not $chocoIds.ContainsKey($package)) { throw "No Chocolatey mapping for $package; install WinGet or extend host-tools" }
            Invoke-Tool 'choco' @('install', $chocoIds[$package], '--yes', '--no-progress')
        } else {
            Invoke-Tool 'winget' @('install', '--id', $package, '--exact', '--source', 'winget', '--silent',
                '--accept-package-agreements', '--accept-source-agreements', '--disable-interactivity')
        }
    } catch { Write-Warning $_; $failed = $true }
}
try {
if (-not (Find-VisualStudio)) {
    $existing = Find-VisualStudio -AnyWorkload
    if ($existing) {
        Invoke-Tool (Join-Path $installerBase 'Microsoft Visual Studio/Installer/setup.exe') @(
            'modify', '--installPath', $existing, '--passive', '--norestart',
            '--add', 'Microsoft.VisualStudio.Workload.VCTools', '--includeRecommended')
    } else {
        if ($useChoco) {
            Invoke-Tool 'choco' @('install', 'visualstudio2022buildtools', 'visualstudio2022-workload-vctools',
                '--yes', '--no-progress')
        } else {
            Invoke-Tool 'winget' @('install', '--id', 'Microsoft.VisualStudio.2022.BuildTools', '--exact', '--source', 'winget',
                '--accept-source-agreements', '--accept-package-agreements', '--override',
                '--wait --passive --norestart --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended')
        }
    }
}
} catch { Write-Warning $_; $failed = $true }
Refresh-ToolPath
try {
if (-not $deferVcpkg) {
if (-not (Test-Path $vcpkgRoot)) { Invoke-Tool 'git' @('clone', 'https://github.com/microsoft/vcpkg.git', $vcpkgRoot) }
if (-not (Test-Path (Join-Path $vcpkgRoot 'vcpkg.exe'))) {
    Invoke-Tool (Join-Path $vcpkgRoot 'bootstrap-vcpkg.bat') @('-disableMetrics')
}
}
} catch { Write-Warning $_; $failed = $true }
if ($mode -eq '--dry-run') { Write-Host "Would write $toolsDir/env.ps1 and audit installed tools."; exit 0 }
function Quote-Literal([string]$Value) { return "'" + $Value.Replace("'", "''") + "'" }
New-Item -ItemType Directory -Path $toolsDir -Force | Out-Null
$content = @()
if (Test-Path (Join-Path $vcpkgRoot 'scripts/buildsystems/vcpkg.cmake')) {
    $content += '$env:VCPKG_ROOT = ' + (Quote-Literal $vcpkgRoot)
}
$content += '$env:PATH = ' + (Quote-Literal (($activationPaths -join ';') + ';')) + ' + $env:PATH'
$vs = Find-VisualStudio
if ($vs) {
    $content += '& ' + (Quote-Literal (Join-Path $vs 'Common7/Tools/Launch-VsDevShell.ps1')) + ' -Arch amd64 -HostArch amd64 -SkipAutomaticLocation'
}
$content | Set-Content -LiteralPath (Join-Path $toolsDir 'env.ps1') -Encoding UTF8
Write-Host "Activate: . '$toolsDir/env.ps1'"
$reportPhase = 'after'
if (-not (Test-HostTools)) { $failed = $true }
if ($failed) { throw 'Setup incomplete; review package errors and missing tools above.' }

exit 0
