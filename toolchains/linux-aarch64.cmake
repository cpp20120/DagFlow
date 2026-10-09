set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)
# Debian/Ubuntu cross GCC supplies its own libc/linker search paths. A custom
# SDK may instead pass -DCMAKE_SYSROOT=/path/to/target-root.
if(CMAKE_SYSROOT)
  list(APPEND CMAKE_FIND_ROOT_PATH "${CMAKE_SYSROOT}")
else()
  list(APPEND CMAKE_FIND_ROOT_PATH /usr/aarch64-linux-gnu)
endif()
include("${CMAKE_CURRENT_LIST_DIR}/TargetRoots.cmake")
