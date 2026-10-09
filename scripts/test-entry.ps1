$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$engine = (Get-Process -Id $PID).Path
$work = Join-Path ([IO.Path]::GetTempPath()) ('dagflow-entry-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null
function Assert([bool]$Value, [string]$Message) { if (-not $Value) { throw $Message } }
try {
    foreach ($source in 'setup.ps1', 'build.ps1', 'setup-host.ps1', 'scripts/entry-options.ps1') {
        $tokens = $null; $errors = $null
        [System.Management.Automation.Language.Parser]::ParseFile((Join-Path $root $source), [ref]$tokens, [ref]$errors) | Out-Null
        Assert ($errors.Count -eq 0) "Parse failed: $source"
    }
    $plan = & $engine -NoProfile -File (Join-Path $root 'setup.ps1') -DryRun -VcpkgRoot (Join-Path $work 'no-sdk') -Run
    Assert ($LASTEXITCODE -eq 0) 'Setup dry-run failed'
    Assert (($plan -join "`n") -match 'PRESETS=core') 'Wrong default preset'
    Assert (-not (Test-Path (Join-Path $work 'no-sdk'))) 'Dry-run installed vcpkg'
    $package = & $engine -NoProfile -File (Join-Path $root 'build.ps1') -DryRun -PackageFormat ZIP -NoTests
    Assert ($LASTEXITCODE -eq 0) 'Package dry-run failed'
    Assert (($package -join "`n") -match 'DAGFLOW_ENABLE_PACKAGING=ON') 'Missing DagFlow package option'
    Assert (($package -join "`n") -match 'RUN_TESTS=OFF') 'NoTests not forwarded'
    $fixture = Join-Path $work "project ' space"
    New-Item -ItemType Directory -Path (Join-Path $fixture 'scripts') -Force | Out-Null
    foreach ($file in 'setup.ps1', 'build.ps1', 'scripts/entry-options.ps1', 'scripts/default-preset.txt') {
        Copy-Item -LiteralPath (Join-Path $root $file) -Destination (Join-Path $fixture $file)
    }
    @'
$ErrorActionPreference = 'Stop'
if ($env:ENTRY_FAIL_HOST) { exit ([int]$env:ENTRY_FAIL_HOST) }
$tools = $args[[Array]::IndexOf($args, '--tools-dir') + 1]
New-Item -ItemType Directory -Path $tools -Force | Out-Null
$ready = Join-Path $tools 'ready'
if (Test-Path $ready) { Write-Host ALREADY } else { Write-Host INSTALLED; Set-Content $ready ready }
$content = 'function global:cmake { Add-Content -LiteralPath $env:ENTRY_TEST_LOG -Value ($args -join "|"); $global:LASTEXITCODE = 0; if ($env:ENTRY_FAIL_BUILD) { $global:LASTEXITCODE = [int]$env:ENTRY_FAIL_BUILD } }'
Set-Content -LiteralPath (Join-Path $tools 'env.ps1') -Value $content
exit 0
'@ | Set-Content -LiteralPath (Join-Path $fixture 'setup-host.ps1')
    $env:ENTRY_TEST_LOG = Join-Path $work 'commands'
    $tools = Join-Path $fixture "tools ' space"
    $first = & $engine -NoProfile -File (Join-Path $fixture 'setup.ps1') -ToolsDir $tools -Run
    Assert ($LASTEXITCODE -eq 0) 'First invocation failed'
    $second = & $engine -NoProfile -File (Join-Path $fixture 'setup.ps1') -ToolsDir $tools -Run
    Assert ($LASTEXITCODE -eq 0) 'Second invocation failed'
    Assert (($first -join "`n") -match 'INSTALLED') 'Missing first installation'
    Assert (($second -join "`n") -match 'ALREADY') 'Existing setup was not reused'
    Assert (@(Get-Content $env:ENTRY_TEST_LOG).Count -eq 2) 'Build did not run twice'
    Remove-Item $env:ENTRY_TEST_LOG
    & $engine -NoProfile -File (Join-Path $fixture 'setup.ps1') -SetupOnly -ToolsDir $tools
    Assert ($LASTEXITCODE -eq 0 -and -not (Test-Path $env:ENTRY_TEST_LOG)) 'SetupOnly invoked build'
    $env:ENTRY_FAIL_HOST = '27'
    & $engine -NoProfile -File (Join-Path $fixture 'setup.ps1')
    Assert ($LASTEXITCODE -eq 27 -and -not (Test-Path $env:ENTRY_TEST_LOG)) 'Host failure did not stop build'
    Remove-Item Env:ENTRY_FAIL_HOST
    $env:ENTRY_FAIL_BUILD = '31'
    & $engine -NoProfile -File (Join-Path $fixture 'setup.ps1')
    Assert ($LASTEXITCODE -eq 31) 'Build failure was swallowed'
    Write-Host 'DagFlow PowerShell entry: activation, reuse, mapping and failure propagation passed.'
} finally {
    Remove-Item Env:ENTRY_FAIL_HOST, Env:ENTRY_FAIL_BUILD, Env:ENTRY_TEST_LOG -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $work -Recurse -Force
}
exit 0
