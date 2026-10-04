# Thin project adapter: the framework owns execution and artifact installation.
if(NOT PRESETS)
  set(PRESETS debug release static-release)
endif()
include("${CMAKE_CURRENT_LIST_DIR}/BuildMatrix.cmake")
