cmake_minimum_required(VERSION 2.8)
if (POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif ()

# ---[ SQLite_VTable project
project(SQLite_VTable CXX C)

# ---[ CTest
include(CTest)

# ---[ Dependencies
find_package(Threads REQUIRED)

# ---[ C++1y Flags
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-std=c++1y" COMPILER_SUPPORTS_CXX1y)

if (COMPILER_SUPPORTS_CXX1y)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
else ()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++1y support. Please use a different C++ compiler.")
endif ()

# Create a new pre-processor macro __VTableFILE__ that has a truncated
# path to reduce the size of the debug log messages.
# Source: http://stackoverflow.com/a/16658858
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__VTableFILE__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"'")

# ---[ Flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra -Werror -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-unused-private-field") #TODO: remove

# -- [ Debug Flags
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fno-omit-frame-pointer -fno-optimize-sibling-calls")

# --[ Output directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ---[ Includes
set(SQLITE_VTABLE_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(SQLITE_VTABLE_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(SQLITE_VTABLE_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
include_directories(${SQLITE_VTABLE_SRC_INCLUDE_DIR} ${SQLITE_VTABLE_TEST_INCLUDE_DIR} ${SQLITE_VTABLE_THIRD_PARTY_INCLUDE_DIR})
include_directories(BEFORE src) # This is needed for gtest.

# ---[ Subdirectories
add_subdirectory(src)
add_subdirectory(test)
