cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(MERCURY_BENCHMARK C)

#------------------------------------------------------------------------------
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if("${MERCURY_BENCHMARK_SOURCE_DIR}" STREQUAL "${MERCURY_BENCHMARK_BINARY_DIR}")
  message(FATAL_ERROR
    "Mercury benchmark requires an out of source Build. "
    "Please create a separate binary directory and run CMake there.")
endif()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# mercury
find_package(MERCURY REQUIRED)
include_directories(${MERCURY_INCLUDE_DIR})

#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
function(build_mercury_benchmark benchmark_name)
  add_executable(${benchmark_name} hg-ctest-util.c ${benchmark_name}.c)
  target_link_libraries(${benchmark_name} mercury)
endfunction()

build_mercury_benchmark(hg-ctest1)
build_mercury_benchmark(hg-ctest2)
build_mercury_benchmark(hg-ctest3)
build_mercury_benchmark(hg-ctest4)
