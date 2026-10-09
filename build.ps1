$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'scripts/entry-options.ps1') -Root $PSScriptRoot -Entry 'build.ps1' -Arguments $args
if ($setupOnly) { throw 'Use setup.ps1 for -SetupOnly.' }
$activation = Join-Path $toolsDir 'env.ps1'
if (-not $dryRun -and (Test-Path -LiteralPath $activation)) { . $activation }
$module = Join-Path $PSScriptRoot 'lib/cmake/build/BuildMatrix.cmake'
if (-not (Test-Path $module)) { $module = Join-Path $PSScriptRoot 'cmake/boilerplate/build/BuildMatrix.cmake' }
$projectOptions = @()
if ($packageArtifacts) {
    $projectOptions += '-DCONFIGURE_ARGS=-DDAGFLOW_ENABLE_PACKAGING=ON;-DDAGFLOW_INSTALL=ON'
} elseif ($installArtifacts) {
    $projectOptions += '-DCONFIGURE_ARGS=-DDAGFLOW_INSTALL=ON'
}
$commandArgs = $projectOptions + @("-DSOURCE_DIR=$PSScriptRoot", "-DPRESETS=$preset", "-DJOBS=$jobs",
    ('-DINSTALL_ARTIFACTS=' + $(if ($installArtifacts) { 'ON' } else { 'OFF' })),
    ('-DRUN_TESTS=' + $(if ($runTests) { 'ON' } else { 'OFF' })),
    ('-DRUN_APPLICATION=' + $(if ($runApplication) { 'ON' } else { 'OFF' })),
    ('-DPACKAGE_ARTIFACTS=' + $(if ($packageArtifacts) { 'ON' } else { 'OFF' })),
    "-DPACKAGE_FORMAT=$packageFormat",
    '-DBOILERPLATE_VCPKG_BOOTSTRAP=ON', "-DRUN_TARGET=$runTarget", '-P', $module)
if ($dryRun) {
    Write-Host ('+ cmake ' + (($commandArgs | ForEach-Object { "'" + $_.Replace("'", "''") + "'" }) -join ' '))
    exit 0
}
if (-not (Get-Command cmake -ErrorAction SilentlyContinue)) { throw 'CMake missing: run ./setup.ps1 first.' }
& cmake @commandArgs
exit $LASTEXITCODE
