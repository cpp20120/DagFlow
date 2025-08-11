cmake_minimum_required(VERSION 3.20)

find_package(Doxygen REQUIRED dot)
find_program(DOXYGEN_EXECUTABLE doxygen REQUIRED)
find_program(DOT_EXECUTABLE dot REQUIRED)

find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(DOXYGEN_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/generate_docs.py")

add_custom_target(
  docs
  COMMAND ${Python3_EXECUTABLE} ${DOXYGEN_SCRIPT}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating documentation using Doxygen"
  VERBATIM)

install(
  DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doxygen_output/html
  DESTINATION share/doc/${PROJECT_NAME}
  COMPONENT docs)

file(
  GLOB_RECURSE
  SOURCE_FILES
  "${CMAKE_SOURCE_DIR}/include/*.h"
  "${CMAKE_SOURCE_DIR}/include/*.hpp"
  "${CMAKE_SOURCE_DIR}/src/*.cpp"
  "${CMAKE_SOURCE_DIR}/lib/*.hpp"
  "${CMAKE_SOURCE_DIR}/lib/*.cpp")

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/doxygen_output/html/index.html
  COMMAND ${Python3_EXECUTABLE} ${DOXYGEN_SCRIPT}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${SOURCE_FILES}
  COMMENT "Generating documentation"
  VERBATIM)