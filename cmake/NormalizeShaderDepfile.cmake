# Some shader compilers emit unescaped spaces in absolute dependency paths.
# Escape known paths/prefixes without parsing whitespace as a filename separator.
# Already escaped compiler output does not match the raw prefix and is preserved.
if(NOT EXISTS "${DEPFILE}")
  message(FATAL_ERROR "Shader compiler did not produce dependency file: ${DEPFILE}")
endif()
file(READ "${DEPFILE}" _original)
set(_content "${_original}")
# Nested prefixes must be handled before their parents.
list(SORT PATHS ORDER DESCENDING)
foreach(_path IN LISTS PATHS)
  if(_path MATCHES " ")
    string(REPLACE " " "\\ " _escaped "${_path}")
    string(REPLACE "${_path}" "${_escaped}" _content "${_content}")
  endif()
endforeach()
if(NOT _content STREQUAL _original)
  file(WRITE "${DEPFILE}" "${_content}")
endif()
