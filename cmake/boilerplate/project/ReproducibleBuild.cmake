include_guard(GLOBAL)
set(BOILERPLATE_SOURCE_DATE_EPOCH "$ENV{SOURCE_DATE_EPOCH}" CACHE STRING
  "SOURCE_DATE_EPOCH recorded for reproducible/distribution builds")

function(_boilerplate_project_reproducible_configure)
  set(BOILERPLATE_REPRODUCIBLE ON CACHE BOOL "Reproducible source/debug path mapping" FORCE)
  if(BOILERPLATE_SOURCE_DATE_EPOCH)
    message(STATUS "Boilerplate: SOURCE_DATE_EPOCH=${BOILERPLATE_SOURCE_DATE_EPOCH}")
  else()
    message(STATUS "Boilerplate: reproducible-build enabled; set SOURCE_DATE_EPOCH for timestamp-stable packaging")
  endif()
endfunction()
