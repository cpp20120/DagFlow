$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'scripts/entry-options.ps1') -Root $PSScriptRoot -Entry 'setup.ps1' -Arguments $args
$mode = if ($dryRun) { '--dry-run' } else { '--install' }
& (Join-Path $PSScriptRoot 'setup-host.ps1') $mode --profile $profile --tools-dir $toolsDir --defer-vcpkg @hostExtra
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if (-not $setupOnly) {
    & (Join-Path $PSScriptRoot 'build.ps1') @buildArgs
    exit $LASTEXITCODE
}
exit 0
