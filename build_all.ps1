& cmake "-DSOURCE_DIR=$PSScriptRoot" @args -P "$PSScriptRoot/cmake/BuildDagFlow.cmake"
exit $LASTEXITCODE
