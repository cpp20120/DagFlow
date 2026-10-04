foreach(_file IN LISTS RUNTIME_FILES)
  file(COPY "${_file}" DESTINATION "${DESTINATION}")
endforeach()
