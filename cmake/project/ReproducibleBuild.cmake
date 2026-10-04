include_guard(GLOBAL)
set(DAGFLOW_SOURCE_DATE_EPOCH "$ENV{SOURCE_DATE_EPOCH}" CACHE STRING
  "SOURCE_DATE_EPOCH recorded for reproducible/distribution builds")

function(_dagflow_project_reproducible_configure)
  set(DAGFLOW_REPRODUCIBLE ON CACHE BOOL "Reproducible source/debug path mapping" FORCE)
  if(DAGFLOW_SOURCE_DATE_EPOCH)
    message(STATUS "DagFlow: SOURCE_DATE_EPOCH=${DAGFLOW_SOURCE_DATE_EPOCH}")
  else()
    message(STATUS "DagFlow: reproducible-build enabled; set SOURCE_DATE_EPOCH for timestamp-stable packaging")
  endif()
endfunction()
