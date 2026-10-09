param([string]$Root, [string]$Entry, [string[]]$Arguments)
$preset = (Get-Content -LiteralPath (Join-Path $Root 'scripts/default-preset.txt') -Raw).Trim()
$profile = 'minimal'; $toolsDir = Join-Path $Root 'out/host-tools'
$jobs = '2'; $runTarget = ''; $dryRun = $false; $setupOnly = $false
$runApplication = $false; $runTests = $true; $installArtifacts = $false; $packageArtifacts = $false; $packageFormat = ''
$hostExtra = @()
$aliases = @{ '-Preset'='--preset'; '-Profile'='--profile'; '-ToolsDir'='--tools-dir';
    '-Jobs'='--jobs'; '-RunTarget'='--run-target'; '-DryRun'='--dry-run';
    '-SetupOnly'='--setup-only'; '-VcpkgRoot'='--vcpkg-root'; '-Help'='--help';
    '-Run'='--run'; '-NoTests'='--no-tests';
    '-InstallArtifacts'='--install-artifacts'; '-Package'='--package'; '-PackageFormat'='--package-format' }
for ($i = 0; $i -lt $Arguments.Count; $i++) {
    $argument = $Arguments[$i]
    if ($aliases.ContainsKey($argument)) { $argument = $aliases[$argument] }
    switch ($argument) {
        { $_ -in '--preset', '--profile', '--tools-dir', '--jobs', '--run-target', '--vcpkg-root', '--package-format' } {
            if ($i + 1 -ge $Arguments.Count -or -not $Arguments[$i + 1] -or $Arguments[$i + 1] -like '--*') { throw "Missing value for $argument" }
            $i++; $value = $Arguments[$i]
            switch ($argument) {
                '--preset' { $preset = $value }; '--profile' { $profile = $value }
                '--tools-dir' { $toolsDir = $value }; '--jobs' { $jobs = $value }
                '--run-target' { $runTarget = $value }
                '--package-format' { $packageFormat = $value; $packageArtifacts = $true }
                default { $hostExtra += @($argument, $value) }
            }
        }
        '--run' { $runApplication = $true }
        '--no-tests' { $runTests = $false }
        '--install-artifacts' { $installArtifacts = $true }
        '--package' { $packageArtifacts = $true }
        '--dry-run' { $dryRun = $true }
        '--setup-only' { $setupOnly = $true }
        { $_ -in '--help', '-h' } {
            Write-Host "Usage: ./$Entry [-Preset NAME] [-Jobs N] [-Run] [-NoTests] [-InstallArtifacts] [-Package [-PackageFormat FORMAT]] [-DryRun]"
            Write-Host 'Tools: -Profile minimal|package|dev|ci -SetupOnly -ToolsDir PATH -VcpkgRoot PATH'
            Write-Host 'Prepares DagFlow tools, builds/tests, optionally runs its example and packages the library.'
            exit 0
        }
        default { throw "Unknown argument: $argument" }
    }
}
if ($preset -notmatch '^[a-zA-Z0-9_-]+$' -or $jobs -notmatch '^[1-9][0-9]*$') { throw 'Invalid preset or jobs' }
if ($runTarget -and $runTarget -notmatch '^[a-zA-Z0-9_.+-]+$') { throw 'Invalid run target' }
if ($profile -notin 'minimal', 'package', 'dev', 'ci') { throw 'Invalid tool profile' }
if ($packageFormat -and $packageFormat -cnotin @('TGZ', 'ZIP', 'DEB', 'RPM', 'NSIS', 'DragNDrop', 'ARCH')) { throw 'Invalid package format' }
if ($packageArtifacts -and $profile -eq 'minimal') { $profile = 'package' }
$buildArgs = @('--preset', $preset, '--jobs', $jobs, '--tools-dir', $toolsDir)
if ($runTarget) { $buildArgs += @('--run-target', $runTarget) }
if ($runApplication) { $buildArgs += '--run' }
if (-not $runTests) { $buildArgs += '--no-tests' }
if ($installArtifacts) { $buildArgs += '--install-artifacts' }
if ($packageArtifacts) { $buildArgs += '--package' }
if ($packageFormat) { $buildArgs += @('--package-format', $packageFormat) }
if ($dryRun) { $buildArgs += '--dry-run' }
