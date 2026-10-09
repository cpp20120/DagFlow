file(GLOB profiles "${PROFILE_DIR}/*.profraw")
if(NOT profiles)
  message(FATAL_ERROR "No .profraw files in ${PROFILE_DIR}; run instrumented training first")
endif()
get_filename_component(output_dir "${PROFILE_OUTPUT}" DIRECTORY)
file(MAKE_DIRECTORY "${output_dir}")
execute_process(COMMAND "${PROFDATA}" merge -o "${PROFILE_OUTPUT}" ${profiles}
                COMMAND_ERROR_IS_FATAL ANY)
message(STATUS "Merged PGO profile: ${PROFILE_OUTPUT}")
